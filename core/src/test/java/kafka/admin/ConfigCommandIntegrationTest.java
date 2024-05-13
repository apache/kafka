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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.security.PasswordEncoder;
import org.apache.kafka.security.PasswordEncoderConfigs;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ZooKeeperInternals;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("deprecation") // Added for Scala 2.12 compatibility for usages of JavaConverters
@ExtendWith(value = ClusterTestExtensions.class)
@Tag("integration")
public class ConfigCommandIntegrationTest {
    AdminZkClient adminZkClient;
    List<String> alterOpts;

    private final ClusterInstance cluster;

    public ConfigCommandIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest(types = {Type.ZK, Type.KRAFT})
    public void testExitWithNonZeroStatusOnUpdatingUnallowedConfig() {
        assertNonZeroStatusExit(Stream.concat(quorumArgs(), Stream.of(
            "--entity-name", cluster.isKRaftTest() ? "0" : "1",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "security.inter.broker.protocol=PLAINTEXT")),
            errOut ->
                assertTrue(errOut.contains("Cannot update these configs dynamically: Set(security.inter.broker.protocol)"), errOut));
    }


    @ClusterTest(types = {Type.ZK})
    public void testExitWithNonZeroStatusOnZkCommandAlterUserQuota() {
        assertNonZeroStatusExit(Stream.concat(quorumArgs(), Stream.of(
            "--entity-type", "users",
            "--entity-name", "admin",
            "--alter", "--add-config", "consumer_byte_rate=20000")),
            errOut ->
                assertTrue(errOut.contains("User configuration updates using ZooKeeper are only supported for SCRAM credential updates."), errOut));
    }

    public static void assertNonZeroStatusExit(Stream<String> args, Consumer<String> checkErrOut) {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });

        String errOut = captureStandardErr(() -> {
            try {
                ConfigCommand.main(args.toArray(String[]::new));
            } catch (RuntimeException e) {
                // do nothing.
            } finally {
                Exit.resetExitProcedure();
            }
        });

        checkErrOut.accept(errOut);
        assertNotNull(exitStatus.get());
        assertEquals(1, exitStatus.get());
    }

    private Stream<String> quorumArgs() {
        return cluster.isKRaftTest()
            ? Stream.of("--bootstrap-server", cluster.bootstrapServers())
            : Stream.of("--zookeeper", ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect());
    }

    public List<String> entityOp(Optional<String> brokerId) {
        return brokerId.map(id -> Arrays.asList("--entity-name", id)).orElse(Collections.singletonList("--entity-default"));
    }

    public void alterConfigWithZk(KafkaZkClient zkClient, Map<String, String> configs, Optional<String> brokerId) throws Exception {
        alterConfigWithZk(zkClient, configs, brokerId, Collections.emptyMap());
    }

    public void alterConfigWithZk(KafkaZkClient zkClient, Map<String, String> configs, Optional<String> brokerId, Map<String, String> encoderConfigs) {
        String configStr = Stream.of(configs.entrySet(), encoderConfigs.entrySet())
            .flatMap(Set::stream)
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(","));
        ConfigCommand.ConfigCommandOptions addOpts = new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(brokerId), Arrays.asList("--add-config", configStr)));
        ConfigCommand.alterConfigWithZk(zkClient, addOpts, adminZkClient);
    }

    void verifyConfig(KafkaZkClient zkClient, Map<String, String> configs, Optional<String> brokerId) {
        Properties entityConfigs = zkClient.getEntityConfigs("brokers", brokerId.orElse(ZooKeeperInternals.DEFAULT_STRING));
        assertEquals(configs, entityConfigs);
    }

    void alterAndVerifyConfig(KafkaZkClient zkClient, Map<String, String> configs, Optional<String> brokerId) throws Exception {
        alterConfigWithZk(zkClient, configs, brokerId);
        verifyConfig(zkClient, configs, brokerId);
    }

    void deleteAndVerifyConfig(KafkaZkClient zkClient, Set<String> configNames, Optional<String> brokerId) {
        ConfigCommand.ConfigCommandOptions deleteOpts = new ConfigCommand.ConfigCommandOptions(toArray(alterOpts, entityOp(brokerId), Arrays.asList("--delete-config", String.join(",", configNames))));
        ConfigCommand.alterConfigWithZk(zkClient, deleteOpts, adminZkClient);
        verifyConfig(zkClient, Collections.emptyMap(), brokerId);
    }

    @ClusterTest(types = {Type.ZK})
    public void testDynamicBrokerConfigUpdateUsingZooKeeper() throws Exception {
        cluster.shutdownBroker(0);
        String zkConnect = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkConnect();
        KafkaZkClient zkClient = ((ZkClusterInvocationContext.ZkClusterInstance) cluster).getUnderlying().zkClient();

        String brokerId = "1";
        adminZkClient = new AdminZkClient(zkClient, scala.None$.empty());
        alterOpts = Arrays.asList("--zookeeper", zkConnect, "--entity-type", "brokers", "--alter");

        // Add config
        alterAndVerifyConfig(zkClient, Collections.singletonMap("message.max.size", "110000"), Optional.of(brokerId));
        alterAndVerifyConfig(zkClient, Collections.singletonMap("message.max.size", "120000"), Optional.empty());

        // Change config
        alterAndVerifyConfig(zkClient, Collections.singletonMap("message.max.size", "130000"), Optional.of(brokerId));
        alterAndVerifyConfig(zkClient, Collections.singletonMap("message.max.size", "140000"), Optional.empty());

        // Delete config
        deleteAndVerifyConfig(zkClient, Collections.singleton("message.max.size"), Optional.of(brokerId));
        deleteAndVerifyConfig(zkClient, Collections.singleton("message.max.size"), Optional.empty());

        // Listener configs: should work only with listener name
        alterAndVerifyConfig(zkClient, Collections.singletonMap("listener.name.external.ssl.keystore.location", "/tmp/test.jks"), Optional.of(brokerId));
        assertThrows(ConfigException.class,
            () -> alterConfigWithZk(zkClient, Collections.singletonMap("ssl.keystore.location", "/tmp/test.jks"), Optional.of(brokerId)));

        // Per-broker config configured at default cluster-level should fail
        assertThrows(ConfigException.class,
            () -> alterConfigWithZk(zkClient, Collections.singletonMap("listener.name.external.ssl.keystore.location", "/tmp/test.jks"), Optional.empty()));
        deleteAndVerifyConfig(zkClient, Collections.singleton("listener.name.external.ssl.keystore.location"), Optional.of(brokerId));

        // Password config update without encoder secret should fail
        assertThrows(IllegalArgumentException.class,
            () -> alterConfigWithZk(zkClient, Collections.singletonMap("listener.name.external.ssl.keystore.password", "secret"), Optional.of(brokerId)));

        // Password config update with encoder secret should succeed and encoded password must be stored in ZK
        Map<String, String> configs = new HashMap<>();
        configs.put("listener.name.external.ssl.keystore.password", "secret");
        configs.put("log.cleaner.threads", "2");
        Map<String, String> encoderConfigs = Collections.singletonMap(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");
        alterConfigWithZk(zkClient, configs, Optional.of(brokerId), encoderConfigs);
        Properties brokerConfigs = zkClient.getEntityConfigs("brokers", brokerId);
        assertFalse(brokerConfigs.contains(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG), "Encoder secret stored in ZooKeeper");
        assertEquals("2", brokerConfigs.getProperty("log.cleaner.threads")); // not encoded
        String encodedPassword = brokerConfigs.getProperty("listener.name.external.ssl.keystore.password");
        PasswordEncoder passwordEncoder = ConfigCommand.createPasswordEncoder(JavaConverters.mapAsScalaMap(encoderConfigs));
        assertEquals("secret", passwordEncoder.decode(encodedPassword).value());
        assertEquals(configs.size(), brokerConfigs.size());

        // Password config update with overrides for encoder parameters
        Map<String, String> configs2 = Collections.singletonMap("listener.name.internal.ssl.keystore.password", "secret2");
        Map<String, String> encoderConfigs2 = new HashMap<>();
        encoderConfigs2.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "encoder-secret");
        encoderConfigs2.put(PasswordEncoderConfigs.PASSWORD_ENCODER_CIPHER_ALGORITHM_CONFIG, "DES/CBC/PKCS5Padding");
        encoderConfigs2.put(PasswordEncoderConfigs.PASSWORD_ENCODER_ITERATIONS_CONFIG, "1024");
        encoderConfigs2.put(PasswordEncoderConfigs.PASSWORD_ENCODER_KEYFACTORY_ALGORITHM_CONFIG, "PBKDF2WithHmacSHA1");
        encoderConfigs2.put(PasswordEncoderConfigs.PASSWORD_ENCODER_KEY_LENGTH_CONFIG, "64");
        alterConfigWithZk(zkClient, configs2, Optional.of(brokerId), encoderConfigs2);
        Properties brokerConfigs2 = zkClient.getEntityConfigs("brokers", brokerId);
        String encodedPassword2 = brokerConfigs2.getProperty("listener.name.internal.ssl.keystore.password");
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(JavaConverters.mapAsScalaMap(encoderConfigs)).decode(encodedPassword2).value());
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(JavaConverters.mapAsScalaMap(encoderConfigs2)).decode(encodedPassword2).value());

        // Password config update at default cluster-level should fail
        assertThrows(ConfigException.class, () -> alterConfigWithZk(zkClient, configs, Optional.empty(), encoderConfigs));

        // Dynamic config updates using ZK should fail if broker is running.
        registerBrokerInZk(zkClient, Integer.parseInt(brokerId));
        assertThrows(IllegalArgumentException.class, () -> alterConfigWithZk(zkClient, Collections.singletonMap("message.max.size", "210000"), Optional.of(brokerId)));
        assertThrows(IllegalArgumentException.class, () -> alterConfigWithZk(zkClient, Collections.singletonMap("message.max.size", "220000"), Optional.empty()));

        // Dynamic config updates using ZK should for a different broker that is not running should succeed
        alterAndVerifyConfig(zkClient, Collections.singletonMap("message.max.size", "230000"), Optional.of("2"));
    }

    private void registerBrokerInZk(KafkaZkClient zkClient, int id) {
        zkClient.createTopLevelPaths();
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        EndPoint endpoint = new EndPoint("localhost", 9092, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
        BrokerInfo brokerInfo = BrokerInfo.apply(Broker.apply(id, seq(endpoint), scala.None$.empty()), MetadataVersion.latestTesting(), 9192);
        zkClient.registerBroker(brokerInfo);
    }

    @SafeVarargs
    static <T> Seq<T> seq(T...seq) {
        return seq(Arrays.asList(seq));
    }

    @SuppressWarnings({"deprecation"})
    static <T> Seq<T> seq(Collection<T> seq) {
        return JavaConverters.asScalaIteratorConverter(seq.iterator()).asScala().toSeq();
    }

    @SafeVarargs
    public static String[] toArray(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toArray(String[]::new);
    }

    public static String captureStandardErr(Runnable runnable) {
        return captureStandardStream(true, runnable);
    }

    private static String captureStandardStream(boolean isErr, Runnable runnable) {
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
