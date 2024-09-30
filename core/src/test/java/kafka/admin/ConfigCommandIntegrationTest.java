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
package kafka.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
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

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(value = ClusterTestExtensions.class)
public class ConfigCommandIntegrationTest {

    private List<String> alterOpts;
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
            "--entity-name", cluster.isKRaftTest() ? "0" : "1",
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
        String message = captureStandardStream(false, run(command));
        assertEquals("Completed updating config for user admin.", message);
    }

    @ClusterTest
    public void testNullStatusOnKraftCommandAlterGroup() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--alter", "--add-config", "consumer.session.timeout.ms=50000"));
        String message = captureStandardStream(false, run(command));
        assertEquals("Completed updating config for group group.", message);

        // Test for the --group alias
        command = Stream.concat(quorumArgs(), Stream.of(
            "--group", "group",
            "--alter", "--add-config", "consumer.session.timeout.ms=50000"));
        message = captureStandardStream(false, run(command));
        assertEquals("Completed updating config for group group.", message);
    }

    @ClusterTest
    public void testNullStatusOnKraftCommandAlterClientMetrics() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
                "--entity-type", "client-metrics",
                "--entity-name", "cm",
                "--alter", "--add-config", "metrics=org.apache"));
        String message = captureStandardStream(false, run(command));
        assertEquals("Completed updating config for client-metric cm.", message);

        // Test for the --client-metrics alias
        command = Stream.concat(quorumArgs(), Stream.of(
                "--client-metrics", "cm",
                "--alter", "--add-config", "metrics=org.apache"));
        message = captureStandardStream(false, run(command));
        assertEquals("Completed updating config for client-metric cm.", message);
    }

    @ClusterTest
    public void testDynamicBrokerConfigUpdateUsingKraft() throws Exception {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            // Add config
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId), singletonMap("message.max.bytes", "110000"));
            alterAndVerifyConfig(client, Optional.empty(), singletonMap("message.max.bytes", "120000"));

            // Change config
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId), singletonMap("message.max.bytes", "130000"));
            alterAndVerifyConfig(client, Optional.empty(), singletonMap("message.max.bytes", "140000"));

            // Delete config
            deleteAndVerifyConfigValue(client, defaultBrokerId, singleton("message.max.bytes"), true);

            // Listener configs: should work only with listener name
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"));
            // Per-broker config configured at default cluster-level should fail
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.empty(),
                            singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"), true));
            deleteAndVerifyConfigValue(client, defaultBrokerId,
                    singleton("listener.name.internal.ssl.keystore.location"), false);
            alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                    singletonMap("listener.name.external.ssl.keystore.password", "secret"), true);

            // Password config update with encoder secret should succeed and encoded password must be stored in ZK
            Map<String, String> configs = new HashMap<>();
            configs.put("listener.name.external.ssl.keystore.password", "secret");
            configs.put("log.cleaner.threads", "2");
            // Password encoder configs
            configs.put(PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");

            // Password config update at default cluster-level should fail
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId), configs, true));
        }
    }

    @ClusterTest
    public void testGroupConfigUpdateUsingKraft() throws Exception {
        alterOpts = asList("--bootstrap-server", cluster.bootstrapServers(), "--entity-type", "groups", "--alter");
        verifyGroupConfigUpdate();

        // Test for the --group alias
        alterOpts = asList("--bootstrap-server", cluster.bootstrapServers(), "--group", "--alter");
        verifyGroupConfigUpdate();
    }

    private void verifyGroupConfigUpdate() throws Exception {
        try (Admin client = cluster.createAdminClient()) {
            // Add config
            Map<String, String> configs = new HashMap<>();
            configs.put("consumer.session.timeout.ms", "50000");
            configs.put("consumer.heartbeat.interval.ms", "6000");
            alterAndVerifyGroupConfig(client, defaultGroupName, configs);

            // Delete config
            configs.put("consumer.session.timeout.ms", "45000");
            configs.put("consumer.heartbeat.interval.ms", "5000");
            deleteAndVerifyGroupConfigValue(client, defaultGroupName, configs);

            // Unknown config configured should fail
            assertThrows(ExecutionException.class,
                () -> alterConfigWithAdmin(client, Optional.of(defaultGroupName),
                    singletonMap("unknown.config", "20000"), true));
        }
    }


    @ClusterTest(types = {Type.KRAFT})
    public void testClientMetricsConfigUpdate() throws Exception {
        alterOpts = asList("--bootstrap-server", cluster.bootstrapServers(), "--entity-type", "client-metrics", "--alter");
        verifyClientMetricsConfigUpdate();

        // Test for the --client-metrics alias
        alterOpts = asList("--bootstrap-server", cluster.bootstrapServers(), "--client-metrics", "--alter");
        verifyClientMetricsConfigUpdate();
    }

    private void verifyClientMetricsConfigUpdate() throws Exception {
        try (Admin client = cluster.createAdminClient()) {
            // Add config
            Map<String, String> configs = new HashMap<>();
            configs.put("metrics", "");
            configs.put("interval.ms", "6000");
            alterAndVerifyClientMetricsConfig(client, defaultClientMetricsName, configs);

            // Delete config
            deleteAndVerifyClientMetricsConfigValue(client, defaultClientMetricsName, configs.keySet());

            // Unknown config configured should fail
            assertThrows(ExecutionException.class,
                () -> alterConfigWithAdmin(client, Optional.of(defaultClientMetricsName),
                    singletonMap("unknown.config", "20000"), true));
        }
    }

    @ClusterTest
    public void testAlterReadOnlyConfigInKRaftThenShouldFail() {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            singletonMap("auto.create.topics.enable", "false"), true));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            singletonMap("auto.leader.rebalance.enable", "false"), true));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            singletonMap("broker.id", "1"), true));
        }
    }

    @ClusterTest
    public void testUpdateClusterWideConfigInKRaftThenShouldSuccessful() throws Exception {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("log.flush.interval.messages", "100"));
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("log.retention.bytes", "20"));
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("log.retention.ms", "2"));
        }
    }

    @ClusterTest
    public void testUpdatePerBrokerConfigWithListenerNameInKRaftThenShouldSuccessful() throws Exception {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());
        String listenerName = "listener.name.internal.";

        try (Admin client = cluster.createAdminClient()) {
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.type", "PKCS12"));
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.location", "/temp/test.jks"));

            alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.password", "password"), true);
            verifyConfigSecretValue(client, Optional.of(defaultBrokerId),
                    singleton(listenerName + "ssl.truststore.password"));
        }
    }

    @ClusterTest
    public void testUpdatePerBrokerConfigInKRaftThenShouldFail() {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            singletonMap("ssl.truststore.type", "PKCS12"), true));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            singletonMap("ssl.truststore.location", "/temp/test.jks"), true));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithAdmin(client, Optional.of(defaultBrokerId),
                            singletonMap("ssl.truststore.password", "password"), true));
        }
    }

    @ClusterTest
    public void testUpdateInvalidBrokerConfigs() {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());
        updateAndCheckInvalidBrokerConfig(Optional.empty(), true);
        updateAndCheckInvalidBrokerConfig(Optional.empty(), false);
        updateAndCheckInvalidBrokerConfig(Optional.of(cluster.anyBrokerSocketServer().config().brokerId() + ""), true);
        updateAndCheckInvalidBrokerConfig(Optional.of(cluster.anyBrokerSocketServer().config().brokerId() + ""), false);
    }

    private void updateAndCheckInvalidBrokerConfig(Optional<String> brokerIdOrDefault, boolean incremental) {
        try (Admin client = cluster.createAdminClient()) {
            alterConfigWithAdmin(client, brokerIdOrDefault, Collections.singletonMap("invalid", "2"), incremental);

            Stream<String> describeCommand = Stream.concat(
                    Stream.concat(
                            Stream.of("--bootstrap-server", cluster.bootstrapServers()),
                            Stream.of(entityOp(brokerIdOrDefault).toArray(new String[0]))),
                    Stream.of("--entity-type", "brokers", "--describe"));
            String describeResult = captureStandardStream(false, run(describeCommand));

            // We will treat unknown config as sensitive
            assertTrue(describeResult.contains("sensitive=true"), describeResult);
            // Sensitive config will not return
            assertTrue(describeResult.contains("invalid=null"), describeResult);
        }
    }

    @ClusterTest
    public void testUpdateInvalidTopicConfigs() throws ExecutionException, InterruptedException {
        alterOpts = asList("--bootstrap-server", cluster.bootstrapServers(), "--entity-type", "topics", "--alter");
        try (Admin client = cluster.createAdminClient()) {
            client.createTopics(Collections.singletonList(new NewTopic("test-config-topic", 1, (short) 1))).all().get();
            assertInstanceOf(
                    InvalidConfigurationException.class,
                    assertThrows(
                            ExecutionException.class,
                            () -> ConfigCommand.alterConfig(
                                    cluster.createAdminClient(),
                                    new ConfigCommand.ConfigCommandOptions(
                                            toArray(alterOpts,
                                                    asList("--add-config", "invalid=2", "--entity-type", "topics", "--entity-name", "test-config-topic"))), true)
                    ).getCause()
            );
        }
    }

    @ClusterTest
    public void testCannotUpdateTopicConfigUsingDeprecatedAlterConfigs() throws ExecutionException, InterruptedException {
        alterOpts = asList("--bootstrap-server", cluster.bootstrapServers(), "--entity-type", "topics", "--alter");
        try (Admin client = cluster.createAdminClient()) {
            client.createTopics(Collections.singletonList(new NewTopic("test-config-topic", 1, (short) 1))).all().get();
            assertThrows(
                    IllegalArgumentException.class,
                    () -> ConfigCommand.alterConfig(
                            cluster.createAdminClient(),
                            new ConfigCommand.ConfigCommandOptions(
                                    toArray(alterOpts,
                                            asList("--alter", "--add-config", "segment.bytes=10240000", "--entity-type", "topics", "--entity-name", "test-config-topic"))),
                            false));
        }
    }


    @ClusterTest
    public void testUpdateBrokerConfigNotAffectedByInvalidConfig() {
        ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommand.ConfigCommandOptions(
                toArray(asList("--bootstrap-server", cluster.bootstrapServers(),
                        "--alter",
                        "--add-config", "log.cleaner.threadzz=2",
                        "--entity-type", "brokers",
                        "--entity-default"))
        ), true);

        ConfigCommand.alterConfig(cluster.createAdminClient(), new ConfigCommand.ConfigCommandOptions(
                toArray(asList("--bootstrap-server", cluster.bootstrapServers(),
                        "--alter",
                        "--add-config", "log.cleaner.threads=2",
                        "--entity-type", "brokers",
                        "--entity-default"))
        ), true);
        kafka.utils.TestUtils.waitUntilTrue(
                () -> cluster.brokerSocketServers().stream().allMatch(broker -> broker.config().getInt("log.cleaner.threads") == 2),
                () -> "Timeout waiting for topic config propagating to broker",
                org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                100L);
    }

    @ClusterTest(types = {Type.KRAFT}, metadataVersion = MetadataVersion.IBP_3_3_IV0) // Zk code has been removed, use kraft and mockito to mock this situation
    public void testFallbackToDeprecatedAlterConfigs() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Admin spyAdmin = Mockito.spy(cluster.createAdminClient());

        AlterConfigsResult mockResult;
        {
            // Create a mock result
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new UnsupportedVersionException("simulated error"));
            Constructor<AlterConfigsResult> constructor = AlterConfigsResult.class.getDeclaredConstructor(java.util.Map.class);
            constructor.setAccessible(true);
            mockResult = constructor.newInstance(Collections.singletonMap(new ConfigResource(ConfigResource.Type.BROKER, ""), future));
            constructor.setAccessible(false);
        }
        Mockito.doReturn(mockResult).when(spyAdmin)
                .incrementalAlterConfigs(any(java.util.Map.class), any(AlterConfigsOptions.class));

        ConfigCommand.alterConfig(spyAdmin, new ConfigCommand.ConfigCommandOptions(
                toArray(asList(
                        "--bootstrap-server", cluster.bootstrapServers(),
                        "--alter",
                        "--add-config", "log.cleaner.threads=2",
                        "--entity-type", "brokers",
                        "--entity-default"))
        ), true);

        Mockito.verify(spyAdmin).alterConfigs(any(java.util.Map.class), any(AlterConfigsOptions.class));

        kafka.utils.TestUtils.waitUntilTrue(
                () -> cluster.brokerSocketServers().stream().allMatch(broker -> broker.config().getInt("log.cleaner.threads") == 2),
                () -> "Timeout waiting for topic config propagating to broker",
                org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                100L);
    }

    private void assertNonZeroStatusExit(Stream<String> args, Consumer<String> checkErrOut) {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });

        String errOut = captureStandardStream(true, run(args));

        checkErrOut.accept(errOut);
        assertNotNull(exitStatus.get());
        assertEquals(1, exitStatus.get());
    }

    private Stream<String> quorumArgs() {
        return Stream.of("--bootstrap-server", cluster.bootstrapServers());
    }

    private List<String> entityOp(Optional<String> entityId) {
        return entityId.map(id -> asList("--entity-name", id))
                .orElse(singletonList("--entity-default"));
    }

    private List<String> generateDefaultAlterOpts(String bootstrapServers) {
        return asList("--bootstrap-server", bootstrapServers,
                "--entity-type", "brokers", "--alter");
    }

    private void alterAndVerifyConfig(Admin client, Optional<String> brokerId, Map<String, String> config) throws Exception {
        alterConfigWithAdmin(client, brokerId, config, true);
        verifyConfig(client, brokerId, config);
    }

    private void alterAndVerifyGroupConfig(Admin client, String groupName, Map<String, String> config) throws Exception {
        alterConfigWithAdmin(client, Optional.of(groupName), config, true);
        verifyGroupConfig(client, groupName, config);
    }

    private void alterAndVerifyClientMetricsConfig(Admin client, String clientMetricsName, Map<String, String> config) throws Exception {
        alterConfigWithAdmin(client, Optional.of(clientMetricsName), config, true);
        verifyClientMetricsConfig(client, clientMetricsName, config);
    }

    private void alterConfigWithAdmin(Admin client, Optional<String> resourceName, Map<String, String> config, boolean incremental) {
        String configStr = transferConfigMapToString(config);
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(resourceName), asList("--add-config", configStr)));
        ConfigCommand.alterConfig(client, addOpts, incremental);
    }

    private void verifyConfig(Admin client, Optional<String> brokerId, Map<String, String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.orElse(""));
        TestUtils.waitForCondition(() -> {
            Map<String, String> current = getConfigEntryStream(client, configResource)
                    .filter(configEntry -> Objects.nonNull(configEntry.value()))
                    .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
            return config.entrySet().stream().allMatch(e -> e.getValue().equals(current.get(e.getKey())));
        }, 10000, config + " are not updated");
    }

    private void verifyGroupConfig(Admin client, String groupName, Map<String, String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupName);
        TestUtils.waitForCondition(() -> {
            Map<String, String> current = getConfigEntryStream(client, configResource)
                .filter(configEntry -> Objects.nonNull(configEntry.value()))
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
            return config.entrySet().stream().allMatch(e -> e.getValue().equals(current.get(e.getKey())));
        }, 10000, config + " are not updated");
    }

    private void verifyClientMetricsConfig(Admin client, String clientMetricsName, Map<String, String> config) throws Exception {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, clientMetricsName);
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
        return client.describeConfigs(singletonList(configResource))
                .all()
                .get()
                .values()
                .stream()
                .flatMap(e -> e.entries().stream());
    }

    private void deleteAndVerifyConfigValue(Admin client,
                                            String brokerId,
                                            Set<String> config,
                                            boolean hasDefaultValue) throws Exception {
        ConfigCommand.ConfigCommandOptions deleteOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, asList("--entity-name", brokerId),
                        asList("--delete-config", String.join(",", config))));
        ConfigCommand.alterConfig(client, deleteOpts, true);
        verifyPerBrokerConfigValue(client, brokerId, config, hasDefaultValue);
    }

    private void deleteAndVerifyGroupConfigValue(Admin client,
                                                 String groupName,
                                                 Map<String, String> defaultConfigs) throws Exception {
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, asList("--entity-name", groupName),
                asList("--delete-config", String.join(",", defaultConfigs.keySet()))));
        ConfigCommand.alterConfig(client, deleteOpts, true);
        verifyGroupConfig(client, groupName, defaultConfigs);
    }

    private void deleteAndVerifyClientMetricsConfigValue(Admin client,
                                                         String clientMetricsName,
                                                         Set<String> defaultConfigs) throws Exception {
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, asList("--entity-name", clientMetricsName),
                asList("--delete-config", String.join(",", defaultConfigs))));
        ConfigCommand.alterConfig(client, deleteOpts, true);
        // There are no default configs returned for client metrics
        verifyClientMetricsConfig(client, clientMetricsName, Collections.emptyMap());
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

    // Copied from ToolsTestUtils.java, can be removed after we move ConfigCommand to tools module
    static String captureStandardStream(boolean isErr, Runnable runnable) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream currentStream = isErr ? System.err : System.out;
        PrintStream tempStream = new PrintStream(outputStream);
        if (isErr)
            System.setErr(tempStream);
        else
            System.setOut(tempStream);
        try {
            runnable.run();
            return outputStream.toString().trim();
        } finally {
            if (isErr)
                System.setErr(currentStream);
            else
                System.setOut(currentStream);

            tempStream.close();
        }
    }
}
