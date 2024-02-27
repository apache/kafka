package org.apache.kafka.tools.config;

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.server.KafkaConfig;
import kafka.server.QuorumTestHarness;
import kafka.utils.TestInfoUtils;
import kafka.zk.AdminZkClient;
import kafka.zk.BrokerInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.security.PasswordEncoder;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.ConfigEntityName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;
import scala.collection.Seq;

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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigCommandIntegrationTest extends QuorumTestHarness {
    /** @see TestInfoUtils#TestWithParameterizedQuorumName()  */
    public static final String TEST_WITH_PARAMETERIZED_QUORUM_NAME = "{displayName}.{argumentsWithNames}";

    AdminZkClient adminZkClient;
    List<String> alterOpts;

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = "zk")
    public void shouldExitWithNonZeroStatusOnUpdatingUnallowedConfigViaZk(String quorum) {
        assertNonZeroStatusExit(
            "--zookeeper", zkConnect(),
            "--entity-name", "1",
            "--entity-type", "brokers",
            "--alter",
            "--add-config", "security.inter.broker.protocol=PLAINTEXT");
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = "zk")
    public void shouldExitWithNonZeroStatusOnZkCommandAlterUserQuota(String quorum) {
        assertNonZeroStatusExit(
            "--zookeeper", zkConnect(),
            "--entity-type", "users",
            "--entity-name", "admin",
            "--alter", "--add-config", "consumer_byte_rate=20000");
    }

    private void assertNonZeroStatusExit(String...args) {
        AtomicReference<Integer> exitStatus = new AtomicReference<>();
        Exit.setExitProcedure((status, __) -> {
            exitStatus.set(status);
            throw new RuntimeException();
        });

        try {
            ConfigCommand.main(args);
        } catch (RuntimeException e) {
            // do nothing.
        } finally {
            Exit.resetExitProcedure();
        }

        assertNotNull(exitStatus.get());
        assertEquals(1, exitStatus.get());
    }

    public List<String> entityOp(Optional<String> brokerId) {
        return brokerId.map(id -> Arrays.asList("--entity-name", id)).orElse(Collections.singletonList("--entity-default"));
    }

    public void alterConfigWithZk(Map<String, String> configs, Optional<String> brokerId) throws Exception {
        alterConfigWithZk(configs, brokerId, Collections.emptyMap());
    }

    public void alterConfigWithZk(Map<String, String> configs, Optional<String> brokerId, Map<String, String> encoderConfigs) throws Exception {
        String configStr = Stream.of(configs.entrySet(), encoderConfigs.entrySet())
            .flatMap(Set::stream)
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(","));
        ConfigCommandOptions addOpts = new ConfigCommandOptions(Stream.of(alterOpts, entityOp(brokerId), Arrays.asList("--add-config", configStr))
            .flatMap(Collection::stream)
            .toArray(String[]::new));
        ConfigCommand.alterConfigWithZk(zkClient(), addOpts, adminZkClient);
    }

    void verifyConfig(Map<String, String> configs, Optional<String> brokerId) {
        Properties entityConfigs = zkClient().getEntityConfigs("brokers", brokerId.orElse(ConfigEntityName.DEFAULT));
        assertEquals(configs, entityConfigs);
    }

    void alterAndVerifyConfig(Map<String, String> configs, Optional<String> brokerId) throws Exception {
        alterConfigWithZk(configs, brokerId);
        verifyConfig(configs, brokerId);
    }

    void deleteAndVerifyConfig(Set<String> configNames, Optional<String> brokerId) throws Exception {
        ConfigCommandOptions deleteOpts = new ConfigCommandOptions(
            Stream.of(alterOpts, entityOp(brokerId), Arrays.asList("--delete-config", String.join(",", configNames)))
                .flatMap(List::stream)
                .toArray(String[]::new));
        ConfigCommand.alterConfigWithZk(zkClient(), deleteOpts, adminZkClient);
        verifyConfig(Collections.emptyMap(), brokerId);
    }

    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = "zk")
    public void testDynamicBrokerConfigUpdateUsingZooKeeper(String quorum) throws Exception {
        String brokerId = "1";
        adminZkClient = new AdminZkClient(zkClient(), scala.None$.empty());
        alterOpts = Arrays.asList("--zookeeper", zkConnect(), "--entity-type", "brokers", "--alter");

        // Add config
        alterAndVerifyConfig(Collections.singletonMap("message.max.size", "110000"), Optional.of(brokerId));
        alterAndVerifyConfig(Collections.singletonMap("message.max.size", "120000"), Optional.empty());

        // Change config
        alterAndVerifyConfig(Collections.singletonMap("message.max.size", "130000"), Optional.of(brokerId));
        alterAndVerifyConfig(Collections.singletonMap("message.max.size", "140000"), Optional.empty());

        // Delete config
        deleteAndVerifyConfig(Collections.singleton("message.max.size"), Optional.of(brokerId));
        deleteAndVerifyConfig(Collections.singleton("message.max.size"), Optional.empty());

        // Listener configs: should work only with listener name
        alterAndVerifyConfig(Collections.singletonMap("listener.name.external.ssl.keystore.location", "/tmp/test.jks"), Optional.of(brokerId));
        assertThrows(ConfigException.class,
            () -> alterConfigWithZk(Collections.singletonMap("ssl.keystore.location", "/tmp/test.jks"), Optional.of(brokerId)));

        // Per-broker config configured at default cluster-level should fail
        assertThrows(ConfigException.class,
            () -> alterConfigWithZk(Collections.singletonMap("listener.name.external.ssl.keystore.location", "/tmp/test.jks"), Optional.empty()));
        deleteAndVerifyConfig(Collections.singleton("listener.name.external.ssl.keystore.location"), Optional.of(brokerId));

        // Password config update without encoder secret should fail
        assertThrows(IllegalArgumentException.class,
            () -> alterConfigWithZk(Collections.singletonMap("listener.name.external.ssl.keystore.password", "secret"), Optional.of(brokerId)));

        // Password config update with encoder secret should succeed and encoded password must be stored in ZK
        Map<String, String> configs = new HashMap<>();
        configs.put("listener.name.external.ssl.keystore.password", "secret");
        configs.put("log.cleaner.threads", "2");
        Map<String, String> encoderConfigs = Collections.singletonMap(KafkaConfig.PasswordEncoderSecretProp(), "encoder-secret");
        alterConfigWithZk(configs, Optional.of(brokerId), encoderConfigs);
        Properties brokerConfigs = zkClient().getEntityConfigs("brokers", brokerId);
        assertFalse(brokerConfigs.contains(KafkaConfig.PasswordEncoderSecretProp()), "Encoder secret stored in ZooKeeper");
        assertEquals("2", brokerConfigs.getProperty("log.cleaner.threads")); // not encoded
        String encodedPassword = brokerConfigs.getProperty("listener.name.external.ssl.keystore.password");
        PasswordEncoder passwordEncoder = ConfigCommand.createPasswordEncoder(encoderConfigs);
        assertEquals("secret", passwordEncoder.decode(encodedPassword).value());
        assertEquals(configs.size(), brokerConfigs.size());

        // Password config update with overrides for encoder parameters
        Map<String, String> configs2 = Collections.singletonMap("listener.name.internal.ssl.keystore.password", "secret2");
        Map<String, String> encoderConfigs2 = new HashMap<>();
        encoderConfigs2.put(KafkaConfig.PasswordEncoderSecretProp(), "encoder-secret");
        encoderConfigs2.put(KafkaConfig.PasswordEncoderCipherAlgorithmProp(), "DES/CBC/PKCS5Padding");
        encoderConfigs2.put(KafkaConfig.PasswordEncoderIterationsProp(), "1024");
        encoderConfigs2.put(KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp(), "PBKDF2WithHmacSHA1");
        encoderConfigs2.put(KafkaConfig.PasswordEncoderKeyLengthProp(), "64");
        alterConfigWithZk(configs2, Optional.of(brokerId), encoderConfigs2);
        Properties brokerConfigs2 = zkClient().getEntityConfigs("brokers", brokerId);
        String encodedPassword2 = brokerConfigs2.getProperty("listener.name.internal.ssl.keystore.password");
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs).decode(encodedPassword2).value());
        assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs2).decode(encodedPassword2).value());

        // Password config update at default cluster-level should fail
        assertThrows(ConfigException.class, () -> alterConfigWithZk(configs, Optional.empty(), encoderConfigs));

        // Dynamic config updates using ZK should fail if broker is running.
        registerBrokerInZk(Integer.parseInt(brokerId));
        assertThrows(IllegalArgumentException.class, () -> alterConfigWithZk(Collections.singletonMap("message.max.size", "210000"), Optional.of(brokerId)));
        assertThrows(IllegalArgumentException.class, () -> alterConfigWithZk(Collections.singletonMap("message.max.size", "220000"), Optional.empty()));

        // Dynamic config updates using ZK should for a different broker that is not running should succeed
        alterAndVerifyConfig(Collections.singletonMap("message.max.size", "230000"), Optional.of("2"));
    }

    private void registerBrokerInZk(int id) {
        zkClient().createTopLevelPaths();
        SecurityProtocol securityProtocol = SecurityProtocol.PLAINTEXT;
        EndPoint endpoint = new EndPoint("localhost", 9092, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol);
        BrokerInfo brokerInfo = BrokerInfo.apply(Broker.apply(id, seq(endpoint), scala.None$.empty()), MetadataVersion.latestTesting(), 9192);
        zkClient().registerBroker(brokerInfo);
    }

    @SuppressWarnings({"deprecation"})
    static <T> Seq<T> seq(T...seq) {
        return JavaConverters.asScalaIteratorConverter(Arrays.asList(seq).iterator()).asScala().toSeq();
    }
}
