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

import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.CONSUMER_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.MESSAGE_MAX_BYTES_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
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
        String message = captureStandardMsg(run(command));

        assertTrue(StringUtils.isBlank(message), message);
    }

    @ClusterTest
    public void testNullStatusOnKraftCommandAlterGroup() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "groups",
            "--entity-name", "group",
            "--alter", "--add-config", "consumer.session.timeout.ms=50000"));
        String message = captureStandardMsg(run(command));
        assertTrue(StringUtils.isBlank(message), message);

        // Test for the --group alias
        command = Stream.concat(quorumArgs(), Stream.of(
            "--group", "group",
            "--alter", "--add-config", "consumer.session.timeout.ms=50000"));
        message = captureStandardMsg(run(command));
        assertTrue(StringUtils.isBlank(message), message);
    }

    @ClusterTest
    public void testNullStatusOnKraftCommandAlterClientMetrics() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
                "--entity-type", "client-metrics",
                "--entity-name", "cm",
                "--alter", "--add-config", "metrics=org.apache"));
        String message = captureStandardMsg(run(command));
        assertTrue(StringUtils.isBlank(message), message);

        // Test for the --client-metrics alias
        command = Stream.concat(quorumArgs(), Stream.of(
                "--client-metrics", "cm",
                "--alter", "--add-config", "metrics=org.apache"));
        message = captureStandardMsg(run(command));
        assertTrue(StringUtils.isBlank(message), message);
    }

    @ClusterTest
    public void testDynamicBrokerConfigUpdateUsingZooKeeper() throws Exception {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();

        String brokerId = "1";
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        List<String> alterOpts = asList("--zookeeper", zkConnect, "--entity-type", "brokers", "--alter");

        try (Admin client = cluster.createAdminClient()) {
            // Add config
            alterAndVerifyConfig(client, adminZkClient, Optional.of(brokerId),
                    singletonMap(MESSAGE_MAX_BYTES_CONFIG, "110000"), alterOpts);
            alterAndVerifyConfig(zkClient, adminZkClient, Optional.empty(),
                    singletonMap(MESSAGE_MAX_BYTES_CONFIG, "120000"), alterOpts);
        }


        // Change config
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singletonMap(MESSAGE_MAX_BYTES_CONFIG, "130000"), alterOpts);
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.empty(),
                singletonMap(MESSAGE_MAX_BYTES_CONFIG, "140000"), alterOpts);

        // Delete config
        deleteAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singleton(MESSAGE_MAX_BYTES_CONFIG), alterOpts);
        deleteAndVerifyConfig(zkClient, adminZkClient, Optional.empty(),
                singleton(MESSAGE_MAX_BYTES_CONFIG), alterOpts);

        // Listener configs: should work only with listener name
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"), alterOpts);
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId),
                        singletonMap(SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/test.jks"), alterOpts));

        // Per-broker config configured at default cluster-level should fail
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.empty(),
                        singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"), alterOpts));
        deleteAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singleton("listener.name.internal.ssl.keystore.location"), alterOpts);

        // Password config update without encoder secret should fail
        assertThrows(IllegalArgumentException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId),
                        singletonMap("listener.name.external.ssl.keystore.password", "secret"), alterOpts));

        // Password config update with encoder secret should succeed and encoded password must be stored in ZK
        Map<String, String> configs = new HashMap<>();
        configs.put("listener.name.external.ssl.keystore.password", "secret");
        configs.put("log.cleaner.threads", "2");
        Map<String, String> encoderConfigs = new HashMap<>(configs);
        encoderConfigs.put(PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");
        alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId), encoderConfigs, alterOpts);
        Properties brokerConfigs = zkClient.getEntityConfigs("brokers", brokerId);
        assertFalse(brokerConfigs.contains(PASSWORD_ENCODER_SECRET_CONFIG), "Encoder secret stored in ZooKeeper");
        assertEquals("2", brokerConfigs.getProperty("log.cleaner.threads")); // not encoded
        String encodedPassword = brokerConfigs.getProperty("listener.name.external.ssl.keystore.password");
        PasswordEncoder passwordEncoder = ConfigCommand.createPasswordEncoder(encoderConfigs);
        assertEquals("secret", passwordEncoder.decode(encodedPassword).value());
        assertEquals(configs.size(), brokerConfigs.size());

        // Password config update with overrides for encoder parameters
        Map<String, String> encoderConfigs2 = generateEncodeConfig();
        alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId), encoderConfigs2, alterOpts);
        Properties brokerConfigs2 = zkClient.getEntityConfigs("brokers", brokerId);
        String encodedPassword2 = brokerConfigs2.getProperty("listener.name.external.ssl.keystore.password");
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs)
                .decode(encodedPassword2).value());
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs2)
                .decode(encodedPassword2).value());

        // Password config update at default cluster-level should fail
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.empty(), encoderConfigs, alterOpts));

        // Dynamic config updates using ZK should fail if broker is running.
        registerBrokerInZk(zkClient, Integer.parseInt(brokerId));
        assertThrows(IllegalArgumentException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId),
                        singletonMap(MESSAGE_MAX_BYTES_CONFIG, "210000"), alterOpts));
        assertThrows(IllegalArgumentException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.empty(),
                        singletonMap(MESSAGE_MAX_BYTES_CONFIG, "220000"), alterOpts));

        // Dynamic config updates using ZK should for a different broker that is not running should succeed
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of("2"),
                singletonMap(MESSAGE_MAX_BYTES_CONFIG, "230000"), alterOpts);
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    @ClusterTest
    public void testDynamicBrokerConfigUpdateUsingKraft() throws Exception {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            // Add config
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId), singletonMap(MESSAGE_MAX_BYTES_CONFIG, "110000"), alterOpts);
            alterAndVerifyConfig(client, Optional.empty(), singletonMap(MESSAGE_MAX_BYTES_CONFIG, "120000"), alterOpts);

            // Change config
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId), singletonMap(MESSAGE_MAX_BYTES_CONFIG, "130000"), alterOpts);
            alterAndVerifyConfig(client, Optional.empty(), singletonMap(MESSAGE_MAX_BYTES_CONFIG, "140000"), alterOpts);

            // Delete config
            deleteAndVerifyConfigValue(client, defaultBrokerId, singleton(MESSAGE_MAX_BYTES_CONFIG), true, alterOpts);

            // Listener configs: should work only with listener name
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"), alterOpts);
            // Per-broker config configured at default cluster-level should fail
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.empty(),
                            singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"), alterOpts));
            deleteAndVerifyConfigValue(client, defaultBrokerId,
                    singleton("listener.name.internal.ssl.keystore.location"), false, alterOpts);
            alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                    singletonMap("listener.name.external.ssl.keystore.password", "secret"), alterOpts);

            // Password config update with encoder secret should succeed and encoded password must be stored in ZK
            Map<String, String> configs = new HashMap<>();
            configs.put("listener.name.external.ssl.keystore.password", "secret");
            configs.put("log.cleaner.threads", "2");
            // Password encoder configs
            configs.put(PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");

            // Password config update at default cluster-level should fail
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId), configs, alterOpts));
        }
    }

    @ClusterTest
    public void testGroupConfigUpdateUsingKraft() throws Exception {
        List<String> alterOpts = Stream.concat(entityOp(Optional.of(defaultGroupName)).stream(),
                        Stream.of("--entity-type", "groups", "--alter"))
                .collect(Collectors.toList());
        verifyGroupConfigUpdate(alterOpts);

        // Test for the --group alias
        verifyGroupConfigUpdate(asList("--group", defaultGroupName, "--alter"));
    }

    private void verifyGroupConfigUpdate(List<String> alterOpts) throws Exception {
        try (Admin client = cluster.createAdminClient()) {
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
            assertThrows(ExecutionException.class, () -> alterConfigWithKraft(client, singletonMap("unknown.config", "20000"), alterOpts));
        }
    }


    @ClusterTest(types = {Type.KRAFT})
    public void testClientMetricsConfigUpdate() throws Exception {
        List<String> alterOpts = Stream.concat(entityOp(Optional.of(defaultClientMetricsName)).stream(),
                        Stream.of("--entity-type", "client-metrics", "--alter"))
            .collect(Collectors.toList());
        verifyClientMetricsConfigUpdate(alterOpts);

        // Test for the --client-metrics alias
        verifyClientMetricsConfigUpdate(asList("--client-metrics", defaultClientMetricsName, "--alter"));
    }

    private void verifyClientMetricsConfigUpdate(List<String> alterOpts) throws Exception {
        try (Admin client = cluster.createAdminClient()) {
            // Add config
            Map<String, String> configs = new HashMap<>();
            configs.put("metrics", "");
            configs.put("interval.ms", "6000");
            alterAndVerifyClientMetricsConfig(client, defaultClientMetricsName, configs, alterOpts);

            // Delete config
            deleteAndVerifyClientMetricsConfigValue(client, defaultClientMetricsName, configs.keySet(), alterOpts);

            // Unknown config configured should fail
            assertThrows(ExecutionException.class, () -> alterConfigWithKraft(client, singletonMap("unknown.config", "20000"), alterOpts));
        }
    }

    @ClusterTest
    public void testAlterReadOnlyConfigInZookeeperThenShouldFail() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        List<String> alterOpts = generateDefaultAlterOpts(zkConnect);

        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap(AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false"), alterOpts));
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap(AUTO_LEADER_REBALANCE_ENABLE_CONFIG, "false"), alterOpts));
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap("broker.id", "1"), alterOpts));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    @ClusterTest
    public void testAlterReadOnlyConfigInKRaftThenShouldFail() {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap(AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap(AUTO_LEADER_REBALANCE_ENABLE_CONFIG, "false"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap("broker.id", "1"), alterOpts));
        }
    }

    @ClusterTest
    public void testUpdateClusterWideConfigInZookeeperThenShouldSuccessful() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        List<String> alterOpts = generateDefaultAlterOpts(zkConnect);

        Map<String, String> configs = new HashMap<>();
        configs.put("log.flush.interval.messages", "100");
        configs.put("log.retention.bytes", "20");
        configs.put("log.retention.ms", "2");

        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId), configs, alterOpts);
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    @ClusterTest
    public void testUpdateClusterWideConfigInKRaftThenShouldSuccessful() throws Exception {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("log.flush.interval.messages", "100"), alterOpts);
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("log.retention.bytes", "20"), alterOpts);
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap("log.retention.ms", "2"), alterOpts);
        }
    }

    @ClusterTest
    public void testUpdatePerBrokerConfigWithListenerNameInZookeeperThenShouldSuccessful() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        List<String> alterOpts = generateDefaultAlterOpts(zkConnect);

        String listenerName = "listener.name.internal.";
        String sslTruststoreType = listenerName + "ssl.truststore.type";
        String sslTruststoreLocation = listenerName + "ssl.truststore.location";
        String sslTruststorePassword = listenerName + "ssl.truststore.password";

        Map<String, String> configs = new HashMap<>();
        configs.put(sslTruststoreType, "PKCS12");
        configs.put(sslTruststoreLocation, "/temp/test.jks");
        configs.put(PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");
        configs.put(sslTruststorePassword, "password");

        alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId), configs, alterOpts);

        Properties properties = zkClient.getEntityConfigs("brokers", defaultBrokerId);
        assertTrue(properties.containsKey(sslTruststorePassword));
        assertEquals(configs.get(sslTruststoreType), properties.getProperty(sslTruststoreType));
        assertEquals(configs.get(sslTruststoreLocation), properties.getProperty(sslTruststoreLocation));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    @ClusterTest
    public void testUpdatePerBrokerConfigWithListenerNameInKRaftThenShouldSuccessful() throws Exception {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());
        String listenerName = "listener.name.internal.";

        try (Admin client = cluster.createAdminClient()) {
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.type", "PKCS12"), alterOpts);
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.location", "/temp/test.jks"), alterOpts);
            alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.password", "password"), alterOpts);
            verifyConfigSecretValue(client, Optional.of(defaultBrokerId),
                    singleton(listenerName + "ssl.truststore.password"));
        }
    }

    @ClusterTest
    public void testUpdatePerBrokerConfigInZookeeperThenShouldFail() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        List<String> alterOpts = generateDefaultAlterOpts(zkConnect);

        assertThrows(ConfigException.class, () ->
                alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap(SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12"), alterOpts));
        assertThrows(ConfigException.class, () ->
                alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap(SSL_TRUSTSTORE_LOCATION_CONFIG, "/temp/test.jks"), alterOpts));
        assertThrows(ConfigException.class, () ->
                alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap(SSL_TRUSTSTORE_PASSWORD_CONFIG, "password"), alterOpts));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    @ClusterTest
    public void testUpdatePerBrokerConfigInKRaftThenShouldFail() {
        List<String> alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap(SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap(SSL_TRUSTSTORE_LOCATION_CONFIG, "/temp/test.jks"), alterOpts));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap(SSL_TRUSTSTORE_PASSWORD_CONFIG, "password"), alterOpts));
        }
    }

    private void assertNonZeroStatusExit(Stream<String> args, Consumer<String> checkErrOut) {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });

        String errOut = captureStandardMsg(run(args));

        checkErrOut.accept(errOut);
        assertNotNull(exitStatus.get());
        assertEquals(1, exitStatus.get());
    }

    private Stream<String> quorumArgs() {
        return Stream.of("--bootstrap-server", cluster.bootstrapServers());
    }

    private void verifyConfig(KafkaZkClient zkClient, Optional<String> brokerId, Map<String, String> config) {
        Properties entityConfigs = zkClient.getEntityConfigs("brokers",
                brokerId.orElse(ZooKeeperInternals.DEFAULT_STRING));
        assertEquals(config, entityConfigs);
    }

    private void alterAndVerifyConfig(KafkaZkClient zkClient,
                                      AdminZkClient adminZkClient,
                                      Optional<String> brokerId,
                                      Map<String, String> configs,
                                      List<String> alterOpts) {
        alterConfigWithZk(zkClient, adminZkClient, brokerId, configs, alterOpts);
        verifyConfig(zkClient, brokerId, configs);
    }

    private void alterConfigWithZk(KafkaZkClient zkClient,
                                   AdminZkClient adminZkClient,
                                   Optional<String> brokerId,
                                   Map<String, String> config,
                                   List<String> alterOpts) {
        String configStr = transferConfigMapToString(config);
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(brokerId), asList("--add-config", configStr)));
        addOpts.checkArgs();
        ConfigCommand.alterConfigWithZk(zkClient, addOpts, adminZkClient);
    }

    private List<String> entityOp(Optional<String> brokerId) {
        return brokerId.map(id -> asList("--entity-name", id))
                .orElse(singletonList("--entity-default"));
    }

    private void deleteAndVerifyConfig(KafkaZkClient zkClient,
                                       AdminZkClient adminZkClient,
                                       Optional<String> brokerId,
                                       Set<String> configNames,
                                       List<String> alterOpts) {
        ConfigCommand.ConfigCommandOptions deleteOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(brokerId),
                        asList("--delete-config", String.join(",", configNames))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfigWithZk(zkClient, deleteOpts, adminZkClient);
        verifyConfig(zkClient, brokerId, Collections.emptyMap());
    }

    private Map<String, String> generateEncodeConfig() {
        Map<String, String> map = new HashMap<>();
        map.put(PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");
        map.put(PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG, "DES/CBC/PKCS5Padding");
        map.put(PASSWORD_ENCODER_ITERATIONS_CONFIG, "1024");
        map.put(PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG, "PBKDF2WithHmacSHA1");
        map.put(PASSWORD_ENCODER_KEY_LENGTH_CONFIG, "64");
        map.put("listener.name.external.ssl.keystore.password", "secret2");
        return map;
    }

    private void registerBrokerInZk(KafkaZkClient zkClient, int id) {
        zkClient.createTopLevelPaths();
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        EndPoint endpoint = new EndPoint("localhost", 9092,
                ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
        BrokerInfo brokerInfo = BrokerInfo.apply(Broker.apply(id, endpoint,
                scala.None$.empty()), MetadataVersion.latestTesting(), 9192);
        zkClient.registerBroker(brokerInfo);
    }

    private List<String> generateDefaultAlterOpts(String bootstrapServers) {
        return asList("--bootstrap-server", bootstrapServers,
                "--entity-type", "brokers", "--alter");
    }

    private void alterAndVerifyConfig(Admin client,
                                      Optional<String> brokerId,
                                      Map<String, String> config,
                                      List<String> alterOpts) throws Exception {
        alterConfigWithKraft(client, brokerId, config, alterOpts);
        verifyConfig(client, brokerId, config);
    }

    private void alterAndVerifyGroupConfig(Admin client,
                                           String groupName,
                                           Map<String, String> config,
                                           List<String> alterOpts) throws Exception {
        alterConfigWithKraft(client, config, alterOpts);
        verifyGroupConfig(client, groupName, config);
    }

    private void alterAndVerifyClientMetricsConfig(Admin client,
                                                   String clientMetricsName,
                                                   Map<String, String> config,
                                                   List<String> alterOpts) throws Exception {
        alterConfigWithKraft(client, config, alterOpts);
        verifyClientMetricsConfig(client, clientMetricsName, config);
    }

    private void alterConfigWithKraft(Admin client, Optional<String> resourceName, Map<String, String> config, List<String> alterOpts) {
        String configStr = transferConfigMapToString(config);
        List<String> bootstrapOpts = quorumArgs().collect(Collectors.toList());
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                        entityOp(resourceName),
                        alterOpts,
                        asList("--add-config", configStr)));
        addOpts.checkArgs();
        ConfigCommand.alterConfig(client, addOpts);
    }

    private void alterConfigWithKraft(Admin client, Map<String, String> config, List<String> alterOpts) {
        String configStr = transferConfigMapToString(config);
        List<String> bootstrapOpts = quorumArgs().collect(Collectors.toList());
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                        alterOpts,
                        asList("--add-config", configStr)));
        addOpts.checkArgs();
        ConfigCommand.alterConfig(client, addOpts);
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
                                            boolean hasDefaultValue,
                                            List<String> alterOpts) throws Exception {
        ConfigCommand.ConfigCommandOptions deleteOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, asList("--entity-name", brokerId),
                        asList("--delete-config", String.join(",", config))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyPerBrokerConfigValue(client, brokerId, config, hasDefaultValue);
    }

    private void deleteAndVerifyGroupConfigValue(Admin client,
                                                 String groupName,
                                                 Map<String, String> defaultConfigs,
                                                 List<String> alterOpts) throws Exception {
        List<String> bootstrapOpts = quorumArgs().collect(Collectors.toList());
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                    alterOpts,
                    asList("--delete-config", String.join(",", defaultConfigs.keySet()))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyGroupConfig(client, groupName, defaultConfigs);
    }

    private void deleteAndVerifyClientMetricsConfigValue(Admin client,
                                                         String clientMetricsName,
                                                         Set<String> defaultConfigs,
                                                         List<String> alterOpts) throws Exception {
        List<String> bootstrapOpts = quorumArgs().collect(Collectors.toList());
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(bootstrapOpts,
                    alterOpts,
                    asList("--delete-config", String.join(",", defaultConfigs))));
        deleteOpts.checkArgs();
        ConfigCommand.alterConfig(client, deleteOpts);
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

    private String captureStandardMsg(Runnable runnable) {
        return captureStandardStream(runnable);
    }

    private String transferConfigMapToString(Map<String, String> configs) {
        return configs.entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(","));
    }

    private String captureStandardStream(Runnable runnable) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream currentStream = System.err;
        try (PrintStream tempStream = new PrintStream(outputStream)) {
            System.setErr(tempStream);
            try {
                runnable.run();
                return outputStream.toString().trim();
            } finally {
                System.setErr(currentStream);
            }
        }
    }
}
