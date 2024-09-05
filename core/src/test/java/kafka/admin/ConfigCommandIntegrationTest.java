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

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import kafka.test.junit.ZkClusterInvocationContext;
import kafka.zk.AdminZkClient;
import kafka.zk.BrokerInfo;
import kafka.zk.KafkaZkClient;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.security.PasswordEncoder;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ZooKeeperInternals;
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
import java.util.Properties;
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
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG;
import static org.apache.kafka.security.PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
public class ConfigCommandIntegrationTest {

    private List<String> alterOpts;
    private final String defaultBrokerId = "0";
    private final String defaultGroupName = "group";
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

    @ClusterTest(types = {Type.ZK, Type.KRAFT, Type.CO_KRAFT})
    public void testExitWithNonZeroStatusOnUpdatingUnallowedConfig() {
        assertNonZeroStatusExit(Stream.concat(quorumArgs(), Stream.of(
            "--entity-name", cluster.isKRaftTest() ? "0" : "1",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "security.inter.broker.protocol=PLAINTEXT")),
            errOut -> assertTrue(errOut.contains("Cannot update these configs dynamically: Set(security.inter.broker.protocol)"), errOut));
    }

    @ClusterTest(types = {Type.ZK})
    public void testExitWithNonZeroStatusOnZkCommandAlterUserQuota() {
        assertNonZeroStatusExit(Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "users",
            "--entity-name", "admin",
            "--alter", "--add-config", "consumer_byte_rate=20000")),
            errOut -> assertTrue(errOut.contains("User configuration updates using ZooKeeper are only supported for SCRAM credential updates."), errOut));
    }

    @ClusterTest(types = {Type.ZK})
    public void testExitWithNonZeroStatusOnZkCommandAlterGroup() {
        assertNonZeroStatusExit(Stream.concat(quorumArgs(), Stream.of(
                "--entity-type", "groups",
                "--entity-name", "group",
                "--alter", "--add-config", "consumer.session.timeout.ms=50000")),
            errOut -> assertTrue(errOut.contains("Invalid entity type groups, the entity type must be one of users, brokers with a --zookeeper argument"), errOut));

        // Test for the --group alias
        assertNonZeroStatusExit(Stream.concat(quorumArgs(), Stream.of(
                "--group", "group",
                "--alter", "--add-config", "consumer.session.timeout.ms=50000")),
            errOut -> assertTrue(errOut.contains("Invalid entity type groups, the entity type must be one of users, brokers with a --zookeeper argument"), errOut));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    public void testNullStatusOnKraftCommandAlterUserQuota() {
        Stream<String> command = Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "users",
            "--entity-name", "admin",
            "--alter", "--add-config", "consumer_byte_rate=20000"));
        String message = captureStandardMsg(run(command));

        assertTrue(StringUtils.isBlank(message), message);
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
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

    @ClusterTest(types = Type.ZK)
    public void testDynamicBrokerConfigUpdateUsingZooKeeper() throws Exception {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();

        String brokerId = "1";
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        alterOpts = asList("--zookeeper", zkConnect, "--entity-type", "brokers", "--alter");

        // Add config
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singletonMap("message.max.bytes", "110000"));
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.empty(),
                singletonMap("message.max.bytes", "120000"));

        // Change config
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singletonMap("message.max.bytes", "130000"));
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.empty(),
                singletonMap("message.max.bytes", "140000"));

        // Delete config
        deleteAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singleton("message.max.bytes"));
        deleteAndVerifyConfig(zkClient, adminZkClient, Optional.empty(),
                singleton("message.max.bytes"));

        // Listener configs: should work only with listener name
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks"));
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId),
                        singletonMap("ssl.keystore.location", "/tmp/test.jks")));

        // Per-broker config configured at default cluster-level should fail
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.empty(),
                        singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks")));
        deleteAndVerifyConfig(zkClient, adminZkClient, Optional.of(brokerId),
                singleton("listener.name.internal.ssl.keystore.location"));

        // Password config update without encoder secret should fail
        assertThrows(IllegalArgumentException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId),
                        singletonMap("listener.name.external.ssl.keystore.password", "secret")));

        // Password config update with encoder secret should succeed and encoded password must be stored in ZK
        Map<String, String> configs = new HashMap<>();
        configs.put("listener.name.external.ssl.keystore.password", "secret");
        configs.put("log.cleaner.threads", "2");
        Map<String, String> encoderConfigs = new HashMap<>(configs);
        encoderConfigs.put(PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");
        alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId), encoderConfigs);
        Properties brokerConfigs = zkClient.getEntityConfigs("brokers", brokerId);
        assertFalse(brokerConfigs.contains(PASSWORD_ENCODER_SECRET_CONFIG), "Encoder secret stored in ZooKeeper");
        assertEquals("2", brokerConfigs.getProperty("log.cleaner.threads")); // not encoded
        String encodedPassword = brokerConfigs.getProperty("listener.name.external.ssl.keystore.password");
        PasswordEncoder passwordEncoder = ConfigCommand.createPasswordEncoder(encoderConfigs);
        assertEquals("secret", passwordEncoder.decode(encodedPassword).value());
        assertEquals(configs.size(), brokerConfigs.size());

        // Password config update with overrides for encoder parameters
        Map<String, String> encoderConfigs2 = generateEncodeConfig();
        alterConfigWithZk(zkClient, adminZkClient, Optional.of(brokerId), encoderConfigs2);
        Properties brokerConfigs2 = zkClient.getEntityConfigs("brokers", brokerId);
        String encodedPassword2 = brokerConfigs2.getProperty("listener.name.external.ssl.keystore.password");
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs)
                .decode(encodedPassword2).value());
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs2)
                .decode(encodedPassword2).value());

        // Password config update at default cluster-level should fail
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.empty(), encoderConfigs));

        // Dynamic config updates using ZK should fail if broker is running.
        registerBrokerInZk(zkClient, Integer.parseInt(brokerId));
        assertThrows(IllegalArgumentException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient,
                        Optional.of(brokerId), singletonMap("message.max.bytes", "210000")));
        assertThrows(IllegalArgumentException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient,
                        Optional.empty(), singletonMap("message.max.bytes", "220000")));

        // Dynamic config updates using ZK should for a different broker that is not running should succeed
        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of("2"), singletonMap("message.max.bytes", "230000"));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
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
                    () -> alterConfigWithKraft(client, Optional.empty(),
                            singletonMap("listener.name.internal.ssl.keystore.location", "/tmp/test.jks")));
            deleteAndVerifyConfigValue(client, defaultBrokerId,
                    singleton("listener.name.internal.ssl.keystore.location"), false);
            alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                    singletonMap("listener.name.external.ssl.keystore.password", "secret"));

            // Password config update with encoder secret should succeed and encoded password must be stored in ZK
            Map<String, String> configs = new HashMap<>();
            configs.put("listener.name.external.ssl.keystore.password", "secret");
            configs.put("log.cleaner.threads", "2");
            // Password encoder configs
            configs.put(PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");

            // Password config update at default cluster-level should fail
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId), configs));
        }
    }

    @ClusterTest(types = {Type.KRAFT, Type.CO_KRAFT})
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
                () -> alterConfigWithKraft(client, Optional.of(defaultGroupName),
                    singletonMap("unknown.config", "20000")));
        }
    }

    @ClusterTest(types = {Type.ZK})
    public void testAlterReadOnlyConfigInZookeeperThenShouldFail() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        alterOpts = generateDefaultAlterOpts(zkConnect);

        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap("auto.create.topics.enable", "false")));
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap("auto.leader.rebalance.enable", "false")));
        assertThrows(ConfigException.class,
                () -> alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap("broker.id", "1")));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    public void testAlterReadOnlyConfigInKRaftThenShouldFail() {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap("auto.create.topics.enable", "false")));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap("auto.leader.rebalance.enable", "false")));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap("broker.id", "1")));
        }
    }

    @ClusterTest(types = {Type.ZK})
    public void testUpdateClusterWideConfigInZookeeperThenShouldSuccessful() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        alterOpts = generateDefaultAlterOpts(zkConnect);

        Map<String, String> configs = new HashMap<>();
        configs.put("log.flush.interval.messages", "100");
        configs.put("log.retention.bytes", "20");
        configs.put("log.retention.ms", "2");

        alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId), configs);
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
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

    @ClusterTest(types = {Type.ZK})
    public void testUpdatePerBrokerConfigWithListenerNameInZookeeperThenShouldSuccessful() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        alterOpts = generateDefaultAlterOpts(zkConnect);

        String listenerName = "listener.name.internal.";
        String sslTruststoreType = listenerName + "ssl.truststore.type";
        String sslTruststoreLocation = listenerName + "ssl.truststore.location";
        String sslTruststorePassword = listenerName + "ssl.truststore.password";

        Map<String, String> configs = new HashMap<>();
        configs.put(sslTruststoreType, "PKCS12");
        configs.put(sslTruststoreLocation, "/temp/test.jks");
        configs.put("password.encoder.secret", "encoder-secret");
        configs.put(sslTruststorePassword, "password");

        alterConfigWithZk(zkClient, adminZkClient, Optional.of(defaultBrokerId), configs);

        Properties properties = zkClient.getEntityConfigs("brokers", defaultBrokerId);
        assertTrue(properties.containsKey(sslTruststorePassword));
        assertEquals(configs.get(sslTruststoreType), properties.getProperty(sslTruststoreType));
        assertEquals(configs.get(sslTruststoreLocation), properties.getProperty(sslTruststoreLocation));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    public void testUpdatePerBrokerConfigWithListenerNameInKRaftThenShouldSuccessful() throws Exception {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());
        String listenerName = "listener.name.internal.";

        try (Admin client = cluster.createAdminClient()) {
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.type", "PKCS12"));
            alterAndVerifyConfig(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.location", "/temp/test.jks"));

            alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                    singletonMap(listenerName + "ssl.truststore.password", "password"));
            verifyConfigSecretValue(client, Optional.of(defaultBrokerId),
                    singleton(listenerName + "ssl.truststore.password"));
        }
    }

    @ClusterTest(types = {Type.ZK})
    public void testUpdatePerBrokerConfigInZookeeperThenShouldFail() {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();
        AdminZkClient adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        alterOpts = generateDefaultAlterOpts(zkConnect);

        assertThrows(ConfigException.class, () ->
                alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap("ssl.truststore.type", "PKCS12")));
        assertThrows(ConfigException.class, () ->
                alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap("ssl.truststore.location", "/temp/test.jks")));
        assertThrows(ConfigException.class, () ->
                alterAndVerifyConfig(zkClient, adminZkClient, Optional.of(defaultBrokerId),
                        singletonMap("ssl.truststore.password", "password")));
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    public void testUpdatePerBrokerConfigInKRaftThenShouldFail() {
        alterOpts = generateDefaultAlterOpts(cluster.bootstrapServers());

        try (Admin client = cluster.createAdminClient()) {
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap("ssl.truststore.type", "PKCS12")));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap("ssl.truststore.location", "/temp/test.jks")));
            assertThrows(ExecutionException.class,
                    () -> alterConfigWithKraft(client, Optional.of(defaultBrokerId),
                            singletonMap("ssl.truststore.password", "password")));
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
        return cluster.isKRaftTest()
                ? Stream.of("--bootstrap-server", cluster.bootstrapServers())
                : Stream.of("--zookeeper", ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect());
    }

    private void verifyConfig(KafkaZkClient zkClient, Optional<String> brokerId, Map<String, String> config) {
        Properties entityConfigs = zkClient.getEntityConfigs("brokers",
                brokerId.orElse(ZooKeeperInternals.DEFAULT_STRING));
        assertEquals(config, entityConfigs);
    }

    private void alterAndVerifyConfig(KafkaZkClient zkClient, AdminZkClient adminZkClient,
                                      Optional<String> brokerId, Map<String, String> configs) {
        alterConfigWithZk(zkClient, adminZkClient, brokerId, configs);
        verifyConfig(zkClient, brokerId, configs);
    }

    private void alterConfigWithZk(KafkaZkClient zkClient, AdminZkClient adminZkClient,
                                   Optional<String> brokerId, Map<String, String> config) {
        String configStr = transferConfigMapToString(config);
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(brokerId), asList("--add-config", configStr)));
        ConfigCommand.alterConfigWithZk(zkClient, addOpts, adminZkClient);
    }

    private List<String> entityOp(Optional<String> brokerId) {
        return brokerId.map(id -> asList("--entity-name", id))
                .orElse(singletonList("--entity-default"));
    }

    private void deleteAndVerifyConfig(KafkaZkClient zkClient, AdminZkClient adminZkClient,
                                       Optional<String> brokerId, Set<String> configNames) {
        ConfigCommand.ConfigCommandOptions deleteOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(brokerId),
                        asList("--delete-config", String.join(",", configNames))));
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

    private void alterAndVerifyConfig(Admin client, Optional<String> brokerId, Map<String, String> config) throws Exception {
        alterConfigWithKraft(client, brokerId, config);
        verifyConfig(client, brokerId, config);
    }

    private void alterAndVerifyGroupConfig(Admin client, String groupName, Map<String, String> config) throws Exception {
        alterConfigWithKraft(client, Optional.of(groupName), config);
        verifyGroupConfig(client, groupName, config);
    }

    private void alterConfigWithKraft(Admin client, Optional<String> resourceName, Map<String, String> config) {
        String configStr = transferConfigMapToString(config);
        ConfigCommand.ConfigCommandOptions addOpts =
                new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(resourceName), asList("--add-config", configStr)));
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
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyPerBrokerConfigValue(client, brokerId, config, hasDefaultValue);
    }

    private void deleteAndVerifyGroupConfigValue(Admin client,
                                                 String groupName,
                                                 Map<String, String> defaultConfigs) throws Exception {
        ConfigCommand.ConfigCommandOptions deleteOpts =
            new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, asList("--entity-name", groupName),
                asList("--delete-config", String.join(",", defaultConfigs.keySet()))));
        ConfigCommand.alterConfig(client, deleteOpts);
        verifyGroupConfig(client, groupName, defaultConfigs);
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
