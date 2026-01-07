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
package org.apache.kafka.tools;

import kafka.admin.ConfigCommand;
import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientTestUtils;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import org.mockito.Mockito;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.MESSAGE_MAX_BYTES_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.apache.kafka.tools.ToolsTestUtils.captureStandardErr;
import static org.apache.kafka.tools.ToolsTestUtils.captureStandardOut;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;

public class ConfigCommandIntegrationTest {
    private final String defaultBrokerId = "0";
    private final String defaultGroupName = "group";
    private final String defaultClientMetricsName = "cm";
    private final ClusterInstance cluster;

    private static Runnable run(Stream<String> command) {
        return () -> {
            try {
                ConfigCommand.main(command.toArray(String[]::new));
            } catch (RuntimeException e) {
                // do nothing.
            } finally {
                Exit.resetExitProcedure();
            }
        };
    }

    public ConfigCommandIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testExitWithNonZeroStatusOnUpdatingUnallowedConfig() {
        assertNonZeroStatusExit(Stream.concat(quorumArgs(), Stream.of(
            "--entity-name", "0",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "security.inter.broker.protocol=PLAINTEXT")),
            errOut -> assertTrue(errOut.contains("Cannot update these configs dynamically: Set(security.inter.broker.protocol)"), errOut));
    }

    @ClusterTest
    public void testNullStatusOnKraftCommandAlterUserQuota() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "users",
            "--entity-name", "admin",
            "--alter", "--add-config", "consumer_byte_rate=20000"));
        String message = captureStandardOut(run(command));
        assertEquals("Completed updating config for user admin.", message);
    }

    @ClusterTest
    public void testNullStatusOnKraftCommandAlterGroup() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--alter", "--add-config", "consumer.session.timeout.ms=50000"));
        String message = captureStandardOut(run(command));
        assertEquals("Completed updating config for group group.", message);

        // Test for the --group alias
        command = Stream.concat(quorumArgs(), Stream.of(
            "--group", "group",
            "--alter", "--add-config", "consumer.session.timeout.ms=50000"));
        message = captureStandardOut(run(command));
        assertEquals("Completed updating config for group group.", message);

        // A non-existent group with dynamic configs can be described
        command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--describe"));
        message = captureStandardOut(run(command));
        assertTrue(message.contains("Dynamic configs for group group are:"));
        assertTrue(message.contains("consumer.session.timeout.ms=50000 sensitive=false synonyms={DYNAMIC_GROUP_CONFIG:consumer.session.timeout.ms=50000}"));

        command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--describe"));
        message = captureStandardOut(run(command));
        assertTrue(message.contains("Dynamic configs for group group are:"));
        assertTrue(message.contains("consumer.session.timeout.ms=50000 sensitive=false synonyms={DYNAMIC_GROUP_CONFIG:consumer.session.timeout.ms=50000}"));
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
    })
    public void testDescribeGroupWithoutDynamicConfigs(ClusterInstance cluster) throws InterruptedException, ExecutionException {
        cluster.createTopic("topic", 1, (short) 1);

        try (Producer<byte[], byte[]> producer = cluster.producer();
             org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> consumer = cluster.consumer(Map.of(
                 "group.protocol", "consumer",
                 "group.id", "group"
             ))) {
            producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>("topic", "key".getBytes(), "value".getBytes())).get();
            producer.flush();
            consumer.subscribe(List.of("topic"));
            consumer.poll(Duration.ofMillis(100));

            TestUtils.waitForCondition(() -> {
                Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
                    "--entity-type", "groups",
                    "--describe"));
                String message = captureStandardOut(run(command));
                return message.contains("Dynamic configs for group group are:");
            }, () -> "cannot describe group without dynamic groups");

            TestUtils.waitForCondition(() -> {
                Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
                    "--entity-type", "groups",
                    "--entity-name", "group",
                    "--describe"));
                String message = captureStandardOut(run(command));
                return message.contains("Dynamic configs for group group are:");
            }, () -> "cannot describe group without dynamic groups");
        }
    }

    @ClusterTest
    public void testNullStatusOnKraftCommandAlterClientMetrics() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
                "--entity-type", "client-metrics",
                "--entity-name", "cm",
                "--alter", "--add-config", "metrics=org.apache"));
        String message = captureStandardOut(run(command));
        assertEquals("Completed updating config for client-metric cm.", message);

        // Test for the --client-metrics alias
        command = Stream.concat(quorumArgs(), Stream.of(
                "--client-metrics", "cm",
                "--alter", "--add-config", "metrics=org.apache"));
        message = captureStandardOut(run(command));
        assertEquals("Completed updating config for client-metric cm.", message);

        command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "client-metrics",
            "--describe"));
        message = captureStandardOut(run(command));
        assertTrue(message.contains("Dynamic configs for client-metric cm are:"));
        assertTrue(message.contains("metrics=org.apache sensitive=false synonyms={DYNAMIC_CLIENT_METRICS_CONFIG:metrics=org.apache}"));

        command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "client-metrics",
            "--entity-name", "cm",
            "--describe"));
        message = captureStandardOut(run(command));
        assertTrue(message.contains("Dynamic configs for client-metric cm are:"));
        assertTrue(message.contains("metrics=org.apache sensitive=false synonyms={DYNAMIC_CLIENT_METRICS_CONFIG:metrics=org.apache}"));
    }

    @ClusterTest
    public void testAddConfigKeyValuesUsingCommand() throws Exception {
        try (Admin client = cluster.admin()) {
            NewTopic newTopic = new NewTopic("topic", 1, (short) 1);
            client.createTopics(Set.of(newTopic)).all().get();
            cluster.waitTopicCreation("topic", 1);
            Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
                    "--entity-type", "topics",
                    "--entity-name", "topic",
                    "--alter", "--add-config", "cleanup.policy=[delete,compact]"));
            String message = captureStandardOut(run(command));
            assertEquals("Completed updating config for topic topic.", message);
            command = Stream.concat(quorumArgs(), Stream.of(
                    "--entity-type", "topics",
                    "--entity-name", "topic",
                    "--describe"));
            message = captureStandardOut(run(command));
            assertTrue(message.contains("cleanup.policy=delete,compact"), "Config entry was not added correctly");
        }
    }

    @ClusterTest
    public void testDynamicBrokerConfigUpdateUsingKraft() throws Exception {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.admin()) {
            // Add config
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId), Map.of(MESSAGE_MAX_BYTES_CONFIG, "110000"), alterOpts);
            alterAndVerifyConfig(client, Optional.empty(), Map.of(MESSAGE_MAX_BYTES_CONFIG, "120000"), alterOpts);

            // Change config
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId), Map.of(MESSAGE_MAX_BYTES_CONFIG, "130000"), alterOpts);
            alterAndVerifyConfig(client, Optional.empty(), Map.of(MESSAGE_MAX_BYTES_CONFIG, "140000"), alterOpts);

            // Delete config
            deleteAndVerifyConfigValue(client, defaultBrokerId, Set.of(MESSAGE_MAX_BYTES_CONFIG), true, alterOpts);

            // Listener configs: should work only with listener name
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    Map.of("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"), alterOpts);
            // Per-broker config configured at default cluster-level should fail
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.empty(),
                            Map.of("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"), alterOpts));
            deleteAndVerifyConfigValue(client, defaultBrokerId,
                    Set.of("listener.name.internal.ssl.keystore.location"), false, alterOpts);
            alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                    Map.of("listener.name.external.ssl.keystore.password", "secret"), alterOpts);

            Map<String, String> configs = new HashMap<>();
            configs.put("listener.name.external.ssl.keystore.password", "secret");
            configs.put("log.cleaner.threads", "2");

            // Password config update at default cluster-level should fail
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId), configs, alterOpts));
        }
    }

    @ClusterTest
    public void testGroupConfigUpdateUsingKraft() throws Exception {
        List<String> alterOpts = Stream.concat(entityOp(Optional.of(defaultGroupName)).stream(),
            Stream.of("--entity-type", "groups", "--alter")).toList();
        verifyGroupConfigUpdate(alterOpts);

        // Test for the --group alias
        verifyGroupConfigUpdate(List.of("--group", defaultGroupName, "--alter"));
    }

    @ClusterTest
    public void testDescribeStreamsGroupConfigs() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--describe", "--all"));
        String message = captureStandardOut(run(command));

        assertTrue(message.contains("streams.heartbeat.interval.ms=5000 sensitive=false synonyms={DEFAULT_CONFIG:streams.heartbeat.interval.ms=5000}"));
        assertTrue(message.contains("streams.num.standby.replicas=0 sensitive=false synonyms={DEFAULT_CONFIG:streams.num.standby.replicas=0}"));
        assertTrue(message.contains("streams.session.timeout.ms=45000 sensitive=false synonyms={DEFAULT_CONFIG:streams.session.timeout.ms=45000}"));
    }

    @ClusterTest
    public void testAlterStreamsGroupNumOfStandbyReplicas() {
        // Verify the initial config
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--describe", "--all"));
        String message = captureStandardOut(run(command));
        assertTrue(message.contains("streams.num.standby.replicas=0"));

        // Alter number of standby replicas
        command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--alter", "--add-config", "streams.num.standby.replicas=1"));
        message = captureStandardOut(run(command));
        assertEquals("Completed updating config for group group.", message);

        // Verify the updated config
        command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--describe"));
        message = captureStandardOut(run(command));
        assertTrue(message.contains("streams.num.standby.replicas=1"));
    }

    private void verifyGroupConfigUpdate(List<String> alterOpts) throws Exception {
        try (Admin client = cluster.admin()) {
            // Add config
            Map<String, String> configs = new HashMap<>();
            configs.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "50000");
            configs.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "6000");
            alterAndVerifyGroupConfig(client, defaultGroupName, configs, alterOpts);

            // Delete config
            configs.put(CONSUMER_SESSION_TIMEOUT_MS_CONFIG, "45000");
            configs.put(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
            deleteAndVerifyGroupConfigValue(client, defaultGroupName, configs, alterOpts);

            // Unknown config configured should fail
            assertThrows(ExecutionException.class, () -> alterConfigWithAdmin(client, Map.of("unknown.config", "20000"), alterOpts));
        }
    }


    @ClusterTest(types = {Type.KRAFT})
    public void testClientMetricsConfigUpdate() throws Exception {
        List<String> alterOpts = Stream.concat(entityOp(Optional.of(defaultClientMetricsName)).stream(),
            Stream.of("--entity-type", "client-metrics", "--alter")).toList();
        verifyClientMetricsConfigUpdate(alterOpts);

        // Test for the --client-metrics alias
        verifyClientMetricsConfigUpdate(List.of("--client-metrics", defaultClientMetricsName, "--alter"));
    }

    private void verifyClientMetricsConfigUpdate(List<String> alterOpts) throws Exception {
        try (Admin client = cluster.admin()) {
            // Add config
            Map<String, String> configs = new HashMap<>();
            configs.put("metrics", "org.apache.kafka.producer.");
            configs.put("interval.ms", "6000");
            alterAndVerifyClientMetricsConfig(client, defaultClientMetricsName, configs, alterOpts);

            // Delete config
            configs.put("metrics", "");
            configs.put("interval.ms", "300000");
            deleteAndVerifyClientMetricsConfigValue(client, defaultClientMetricsName, configs, alterOpts);

            // Unknown config configured should fail
            assertThrows(ExecutionException.class, () -> alterConfigWithAdmin(client, Map.of("unknown.config", "20000"), alterOpts));
        }
    }

    @ClusterTest
    public void testBrokerLoggerConfigUpdate() throws Exception {
        List<String> alterOpts = Stream.concat(entityOp(Optional.of(defaultBrokerId)).stream(),
            Stream.of("--entity-type", "broker-loggers", "--alter")).toList();

        verifyBrokerLoggerConfigUpdate(alterOpts);

        // Test for the --broker-logger alias
        verifyBrokerLoggerConfigUpdate(List.of("--broker-logger", defaultBrokerId, "--alter"));
    }

    private void verifyBrokerLoggerConfigUpdate(List<String> alterOpts) throws Exception {
        try (Admin client = cluster.admin()) {
            // Add config
            Map<String, String> configs = new HashMap<>();
            configs.put("org.apache.kafka.server.quota.ClientQuotaManager$ThrottledChannelReaper", "DEBUG");
            alterAndVerifyBrokerLoggerConfig(client, defaultBrokerId, configs, alterOpts);

            // Delete config
            configs.put("org.apache.kafka.server.quota.ClientQuotaManager$ThrottledChannelReaper", "ERROR");
            deleteAndVerifyBrokerLoggerConfigValue(client, defaultBrokerId, configs, alterOpts);
        }
    }

    @ClusterTest
    public void testAlterReadOnlyConfigInKRaftThenShouldFail() {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.admin()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            Map.of(AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            Map.of(AUTO_LEADER_REBALANCE_ENABLE_CONFIG, "false"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            Map.of("broker.id", "1"), alterOpts));
        }
    }

    @ClusterTest
    public void testUpdateClusterWideConfigInKRaftThenShouldSuccessful() throws Exception {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.admin()) {
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    Map.of("log.flush.interval.messages", "100"), alterOpts);
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    Map.of("log.retention.bytes", "20"), alterOpts);
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    Map.of("log.retention.ms", "2"), alterOpts);
        }
    }

    @ClusterTest
    public void testUpdatePerBrokerConfigWithListenerNameInKRaftThenShouldSuccessful() throws Exception {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());
        String listenerName = "listener.name.internal.";

        try (Admin client = cluster.admin()) {
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    Map.of(listenerName + "ssl.truststore.type", "PKCS12"), alterOpts);
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    Map.of(listenerName + "ssl.truststore.location", "/temp/test.jks"), alterOpts);
            alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                    Map.of(listenerName + "ssl.truststore.password", "password"), alterOpts);
            verifyConfigSecretValue(client, Optional.of(defaultBrokerId),
                    Set.of(listenerName + "ssl.truststore.password"));
        }
    }

    @ClusterTest
    public void testUpdatePerBrokerConfigInKRaftThenShouldFail() {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.admin()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            Map.of(SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            Map.of(SSL_TRUSTSTORE_LOCATION_CONFIG, "/temp/test.jks"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            Map.of(SSL_TRUSTSTORE_PASSWORD_CONFIG, "password"), alterOpts));
        }
    }

    @ClusterTest
    public void testUpdateInvalidBrokerConfigs() {
        updateAndCheckInvalidBrokerConfig(Optional.empty());
        updateAndCheckInvalidBrokerConfig(Optional.of(String.valueOf((cluster.brokers().entrySet().iterator().next().getKey()))));
    }

    private void updateAndCheckInvalidBrokerConfig(Optional<String> brokerIdOrDefault) {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());
        try (Admin client = cluster.admin()) {
            alterConfigWithAdmin(client, brokerIdOrDefault, Map.of("invalid", "2"), alterOpts);

            Stream<String> describeCommand = Stream.concat(
                    Stream.concat(
                            Stream.of("--bootstrap-server", cluster.bootstrapServers()),
                            Stream.of(entityOp(brokerIdOrDefault).toArray(new String[0]))),
                    Stream.of("--entity-type", "brokers", "--describe"));
            String describeResult = captureStandardOut(run(describeCommand));

            // We will treat unknown config as sensitive
            assertTrue(describeResult.contains("sensitive=true"), describeResult);
            // Sensitive config will not return
            assertTrue(describeResult.contains("invalid=null"), describeResult);
        }
    }

    @ClusterTest
    public void testUpdateInvalidTopicConfigs() throws ExecutionException, InterruptedException {
        List<String> alterOpts = List.of("--bootstrap-server", cluster.bootstrapServers(), "--entity-type", "topics", "--alter");
        try (Admin client = cluster.admin()) {
            client.createTopics(List.of(new NewTopic("test-config-topic", 1, (short) 1))).all().get();
            assertInstanceOf(
                    InvalidConfigurationException.class,
                    assertThrows(
                            ExecutionException.class,
                            () -> ConfigCommand.alterConfig(
                                    client,
                                    new ConfigCommand.ConfigCommandOptions(
                                            toArray(alterOpts,
                                                    List.of("--add-config", "invalid=2", "--entity-type", "topics", "--entity-name", "test-config-topic"))))
                    ).getCause()
            );
        }
    }

    // Test case from KAFKA-13788
    @ClusterTest(serverProperties = {
        // Must be at greater than 1MB per cleaner thread, set to 2M+2 so that we can set 2 cleaner threads.
        @ClusterConfigProperty(key = "log.cleaner.dedupe.buffer.size", value = "2097154"),
    })
    public void testUpdateBrokerConfigNotAffectedByInvalidConfig() {
        try (Admin client = cluster.admin()) {
            ConfigCommand.alterConfig(client, new ConfigCommand.ConfigCommandOptions(
                    toArray(List.of("--bootstrap-server", cluster.bootstrapServers(),
                            "--alter",
                            "--add-config", "log.cleaner.threadzz=2",
                            "--entity-type", "brokers",
                            "--entity-default"))));

            ConfigCommand.alterConfig(client, new ConfigCommand.ConfigCommandOptions(
                    toArray(List.of("--bootstrap-server", cluster.bootstrapServers(),
                            "--alter",
                            "--add-config", "log.cleaner.threads=2",
                            "--entity-type", "brokers",
                            "--entity-default"))));
            kafka.utils.TestUtils.waitUntilTrue(
                    () -> cluster.brokers().values().stream()
                        .map(KafkaBroker::config)
                        .allMatch(config -> config.getInt("log.cleaner.threads") == 2),
                    () -> "Timeout waiting for topic config propagating to broker",
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                    100L);
        }
    }

    @ClusterTest(
         // Must be at greater than 1MB per cleaner thread, set to 2M+2 so that we can set 2 cleaner threads.
         serverProperties = {@ClusterConfigProperty(key = "log.cleaner.dedupe.buffer.size", value = "2097154")},
         metadataVersion = MetadataVersion.IBP_3_9_IV0
    )
    public void testUnsupportedVersionException() {
        try (Admin client = cluster.admin()) {
            Admin spyAdmin = Mockito.spy(client);

            AlterConfigsResult mockResult = AdminClientTestUtils.alterConfigsResult(
                    new ConfigResource(ConfigResource.Type.BROKER, ""), new UnsupportedVersionException("simulated error"));
            Mockito.doReturn(mockResult).when(spyAdmin)
                    .incrementalAlterConfigs(anyMap(), any(AlterConfigsOptions.class));
            assertEquals(
                    "The INCREMENTAL_ALTER_CONFIGS API is not supported by the cluster. The API is supported starting from version 2.3.0. You may want to use an older version of this tool to interact with your cluster, or upgrade your brokers to version 2.3.0 or newer to avoid this error.",
                    assertThrows(UnsupportedVersionException.class, () ->
                        ConfigCommand.alterConfig(spyAdmin, new ConfigCommand.ConfigCommandOptions(
                                toArray(List.of(
                                        "--bootstrap-server", cluster.bootstrapServers(),
                                        "--alter",
                                        "--add-config", "log.cleaner.threads=2",
                                        "--entity-type", "brokers",
                                        "--entity-default"))))
                    ).getMessage()
            );
            Mockito.verify(spyAdmin).incrementalAlterConfigs(anyMap(), any(AlterConfigsOptions.class));
        }
    }

    @ClusterTest
    public void testDescribeNonExistentConfigResource() {
        Map<String, String> configResourceTypeAndNames = Map.of(
            "brokers", "3",
            "broker-loggers", "3",
            "topics", "non-existent",
            "groups", "non-existent",
            "client-metrics", "non-existent");
        configResourceTypeAndNames.forEach((type, name) -> {
            Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
                "--entity-type", type,
                "--entity-name", name,
                "--describe"));
            String message = captureStandardOut(run(command));
            assertTrue(
                message.contains("The " + type.substring(0, type.length() - 1) + " '" + name + "' doesn't exist and doesn't have dynamic config."),
                "The config resource type " + type + " got unexpected result: " + message
            );
        });
    }

    private void assertNonZeroStatusExit(Stream<String> args, Consumer<String> checkErrOut) {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });

        String errOut = captureStandardErr(run(args));

        checkErrOut.accept(errOut);
        assertNotNull(exitStatus.get());
        assertEquals(1, exitStatus.get());
    }

    private Stream<String> quorumArgs() {
        return Stream.of("--bootstrap-server", cluster.bootstrapServers());
    }

    private List<String> entityOp(Optional<String> entityId) {
        return entityId.map(id -> List.of("--entity-name", id))
                .orElse(List.of("--entity-default"));
    }

    private List<String> generateDefaultAlterOpts(String bootstrapServers) {
        return List.of("--bootstrap-server", bootstrapServers,
                "--entity-type", "brokers", "--alter");
    }

    private void alterAndVerifyConfig(Admin client,
                                      Optional<String> brokerId,
                                      Map<String, String> config,
                                      List<String> alterOpts) throws Exception {
        alterConfigWithAdmin(client, brokerId, config, alterOpts);
        verifyBrokerConfig(client, brokerId, config);
    }

    private void alterAndVerifyGroupConfig(Admin client,
                                           String groupName,
                                           Map<String, String> config,
                                           List<String> alterOpts) throws Exception {
        alterConfigWithAdmin(client, config, alterOpts);
        verifyGroupConfig(client, groupName, config);
    }

    private void alterAndVerifyClientMetricsConfig(Admin client,
                                                   String clientMetricsName,
                                                   Map<String, String> config,
                                                   List<String> alterOpts) throws Exception {
        alterConfigWithAdmin(client, config, alterOpts);
        verifyClientMetricsConfig(client, clientMetricsName, config);
    }

    private void alterAndVerifyBrokerLoggerConfig(Admin client,
                                                  String brokerId,
                                                  Map<String, String> config,
                                                  List<String> alterOpts) throws Exception {
        alterConfigWithAdmin(client, config, alterOpts);
        verifyBrokerLoggerConfig(client, brokerId, config);
    }

    private void alterConfigWithAdmin(Admin client, Optional<String> resourceName, Map<String, String> config, List<String> alterOpts) {
        String configStr = transferConfigMapToString(config);
        List<String> bootstrapOpts = quorumArgs().toList();
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                        entityOp(resourceName),
                        alterOpts,
                        List.of("--add-config", configStr)));
        addOpts.checkArgs();
        ConfigCommand.alterConfig(client, addOpts);
    }

    private void alterConfigWithAdmin(Admin client, Map<String, String> config, List<String> alterOpts) {
        String configStr = transferConfigMapToString(config);
        List<String> bootstrapOpts = quorumArgs().toList();
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                        alterOpts,
                        List.of("--add-config", configStr)));
        addOpts.checkArgs();
        ConfigCommand.alterConfig(client, addOpts);
    }

    private void verifyBrokerConfig(Admin client, Optional<String> brokerId, Map<String, String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.orElse(""));
        verifyConfig(client, config, configResource);
    }

    private void verifyGroupConfig(Admin client, String groupName, Map<String, String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupName);
        verifyConfig(client, config, configResource);
    }

    private void verifyClientMetricsConfig(Admin client, String clientMetricsName, Map<String, String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
        verifyConfig(client, config, configResource);
    }

    private void verifyBrokerLoggerConfig(Admin client, String brokerId, Map<String, String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, brokerId);
        verifyConfig(client, config, configResource);
    }

    private void verifyConfig(Admin client, Map<String, String> config, ConfigResource configResource) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            Map<String, String> current = getConfigEntryStream(client, configResource)
                    .filter(configEntry -> Objects.nonNull(configEntry.value()))
                    .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
            if (config.isEmpty())
                return current.isEmpty();
            return config.entrySet().stream().allMatch(e -> e.getValue().equals(current.get(e.getKey())));
        }, 10000, config + " are not updated");
    }

    private Stream<ConfigEntry> getConfigEntryStream(Admin client,
                                                     ConfigResource configResource) throws InterruptedException, ExecutionException {
        return client.describeConfigs(List.of(configResource))
                .all()
                .get()
                .values()
                .stream()
                .flatMap(e -> e.entries().stream());
    }

    private void deleteAndVerifyConfigValue(Admin client,
                                            String brokerId,
                                            Set<String> config,
                                            boolean hasDefaultValue,
                                            List<String> alterOpts) throws Exception {
        ConfigCommand.ConfigCommandOptions deleteOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, List.of("--entity-name", brokerId),
                        List.of("--delete-config", String.join(",", config))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyPerBrokerConfigValue(client, brokerId, config, hasDefaultValue);
    }

    private void deleteAndVerifyGroupConfigValue(Admin client,
                                                 String groupName,
                                                 Map<String, String> defaultConfigs,
                                                 List<String> alterOpts) throws Exception {
        List<String> bootstrapOpts = quorumArgs().toList();
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                    alterOpts,
                    List.of("--delete-config", String.join(",", defaultConfigs.keySet()))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyGroupConfig(client, groupName, defaultConfigs);
    }

    private void deleteAndVerifyClientMetricsConfigValue(Admin client,
                                                         String clientMetricsName,
                                                         Map<String, String> defaultConfigs,
                                                         List<String> alterOpts) throws Exception {
        List<String> bootstrapOpts = quorumArgs().toList();
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                    alterOpts,
                    List.of("--delete-config", String.join(",", defaultConfigs.keySet()))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyClientMetricsConfig(client, clientMetricsName, defaultConfigs);
    }

    private void deleteAndVerifyBrokerLoggerConfigValue(Admin client,
                                                        String brokerId,
                                                        Map<String, String> defaultConfigs,
                                                        List<String> alterOpts) throws Exception {
        List<String> bootstrapOpts = quorumArgs().toList();
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                    alterOpts,
                    List.of("--delete-config", String.join(",", defaultConfigs.keySet()))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyBrokerLoggerConfig(client, brokerId, defaultConfigs);
    }

    private void verifyPerBrokerConfigValue(Admin client,
                                            String brokerId,
                                            Set<String> config,
                                            boolean hasDefaultValue) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId);
        TestUtils.waitForCondition(() -> {
            if (hasDefaultValue) {
                Map<String, String> current = getConfigEntryStream(client, configResource)
                        .filter(configEntry -> Objects.nonNull(configEntry.value()))
                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
                return config.stream().allMatch(current::containsKey);
            } else {
                return getConfigEntryStream(client, configResource)
                        .noneMatch(configEntry -> config.contains(configEntry.name()));
            }
        }, 5000, config + " are not updated");
    }

    private void verifyConfigSecretValue(Admin client, Optional<String> brokerId, Set<String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.orElse(""));
        TestUtils.waitForCondition(() -> {
            Map<String, String> current = getConfigEntryStream(client, configResource)
                    .filter(ConfigEntry::isSensitive)
                    .collect(HashMap::new, (map, entry) -> map.put(entry.name(), entry.value()), HashMap::putAll);
            return config.stream().allMatch(current::containsKey);
        }, 5000, config + " are not updated");
    }

    @SafeVarargs
    private static String[] toArray(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toArray(String[]::new);
    }

    private String transferConfigMapToString(Map<String, String> configs) {
        return configs.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));
    }
}
