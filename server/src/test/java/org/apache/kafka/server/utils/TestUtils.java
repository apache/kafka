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
package org.apache.kafka.server.utils;

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.config.KafkaConfig;
import org.apache.kafka.test.TestSslUtils;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TestUtils {
    /* 0 gives a random port; you can then retrieve the assigned port from the Socket object. */
    public final static int RANDOM_PORT = 0;
    /* Incorrect broker port which can used by kafka clients in tests. This port should not be used
     by any other service and hence we use a reserved port. */
    public final static int INCORRECT_BROKER_PORT = 225;
    /**
     * Port to use for unit tests that mock/don't require a real ZK server.
     */
    public final static int MOCK_ZK_PORT = 1;
    /**
     * ZooKeeper connection string to use for unit tests that mock/don't require a real ZK server.
     */
    public final static String MOCK_ZK_CONNECT = "127.0.0.1:" + MOCK_ZK_PORT;
    // CN in SSL certificates - this is used for endpoint validation when enabled
    public final static String SSL_CERTIFICATE_CN = "localhost";

    /**
     * Create a test config for the provided parameters.
     * <p>
     * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
     */
    public static Properties createBrokerConfig(int nodeId,
                                                String zkConnect,
                                                int port) {
        BrokerConfigBuilder.Builder builder = new BrokerConfigBuilder.Builder(nodeId, zkConnect).port(port);
        return createBrokerConfig(builder.build());
    }

    public static Properties createBrokerConfig(int nodeId, String zkConnect) {
        return createBrokerConfig(nodeId, zkConnect, RANDOM_PORT);
    }

    public static Properties createBrokerConfig(BrokerConfigBuilder brokerConfig) {
        List<Map.Entry<SecurityProtocol, Integer>> protocolAndPorts =
                setupProtocolAndPorts(brokerConfig);

        String listeners = protocolAndPorts.stream()
                .map(entry -> entry.getKey().name() + "://localhost:" + entry.getValue())
                .reduce((s1, s2) -> s1 + "," + s2)
                .orElse("");

        Properties props = setupBaseConfig(brokerConfig);

        if (brokerConfig.zkConnect == null) {
            setKRAFTModeConfig(brokerConfig, props, listeners, protocolAndPorts);
        } else {
            setZKModeConfig(brokerConfig, listeners, props);
        }

        setupLogDirs(brokerConfig.logDirCount, props);

        props.putIfAbsent(KafkaConfig.OFFSETS_TOPIC_PARTITIONS_PROP, "5");
        props.putIfAbsent(KafkaConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_PROP, "0");
        brokerConfig.rack.ifPresent(rackValue -> props.put(KafkaConfig.RACK_PROP, rackValue));

        setupSslConfigsAndJaas(brokerConfig, protocolAndPorts, props);

        brokerConfig.interBrokerSecurityProtocol.ifPresent(protocol ->
                props.put(KafkaConfig.INTER_BROKER_SECURITY_PROTOCOL_PROP, protocol.name()));

        setupTokenAndFetchFromFollower(brokerConfig, props);

        return props;
    }

    private static void setupTokenAndFetchFromFollower(BrokerConfigBuilder brokerConfig, Properties props) {
        if (brokerConfig.enableToken)
            props.put(KafkaConfig.DELEGATION_TOKEN_SECRET_KEY_PROP, "secretkey");

        if (brokerConfig.enableFetchFromFollower) {
            props.put(KafkaConfig.RACK_PROP, Integer.toString(brokerConfig.nodeId));
            props.put(KafkaConfig.REPLICA_SELECTOR_CLASS_PROP, "org.apache.kafka.common.replica.RackAwareReplicaSelector");
        }
    }

    private static void setupSslConfigsAndJaas(BrokerConfigBuilder brokerConfig, List<Map.Entry<SecurityProtocol, Integer>> protocolAndPorts, Properties props) {
        protocolAndPorts.stream()
                .filter(entry -> usesSslTransportLayer(entry.getKey()))
                .findFirst()
                .ifPresent(entry ->
                        props.putAll(sslConfigs(Mode.SERVER, false, brokerConfig.trustStoreFile, "server" + brokerConfig.nodeId, SSL_CERTIFICATE_CN, TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS)));

        protocolAndPorts.stream()
                .filter(entry -> usesSaslAuthentication(entry.getKey()))
                .findFirst()
                .ifPresent(entry ->
                        props.putAll(JaasTestUtils.saslConfigs(brokerConfig.saslProperties)));
    }

    private static void setupLogDirs(int logDirCount, Properties props) {
        if (logDirCount > 1) {
            String logDirs = IntStream.rangeClosed(1, logDirCount)
                    .mapToObj(i -> {
                        // We would like to allow user to specify both relative path and absolute path as log directory for backward-compatibility reason
                        // We can verify this by using a mixture of relative path and absolute path as log directories in the test
                        return i % 2 == 0 ? tempDir().getAbsolutePath() : tempRelativeDir("data").getAbsolutePath();
                    })
                    .collect(Collectors.joining(","));
            props.put(KafkaConfig.LOG_DIRS_PROP, logDirs);
        } else {
            props.put(KafkaConfig.LOG_DIR_PROP, tempDir().getAbsolutePath());
        }
    }

    private static List<Map.Entry<SecurityProtocol, Integer>> setupProtocolAndPorts(BrokerConfigBuilder brokerConfig) {
        List<Map.Entry<SecurityProtocol, Integer>> protocolAndPorts = new ArrayList<>();

        if (brokerConfig.enablePlaintext || shouldEnable(brokerConfig.interBrokerSecurityProtocol, SecurityProtocol.PLAINTEXT))
            protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.PLAINTEXT, brokerConfig.port));

        if (brokerConfig.enableSsl || shouldEnable(brokerConfig.interBrokerSecurityProtocol, SecurityProtocol.SSL))
            protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.SSL, brokerConfig.sslPort));

        if (brokerConfig.enableSaslPlaintext || shouldEnable(brokerConfig.interBrokerSecurityProtocol, SecurityProtocol.SASL_PLAINTEXT))
            protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.SASL_PLAINTEXT, brokerConfig.saslPlaintextPort));

        if (brokerConfig.enableSaslSsl || shouldEnable(brokerConfig.interBrokerSecurityProtocol, SecurityProtocol.SASL_SSL))
            protocolAndPorts.add(new AbstractMap.SimpleEntry<>(SecurityProtocol.SASL_SSL, brokerConfig.saslSslPort));
        return protocolAndPorts;
    }

    private static void setZKModeConfig(BrokerConfigBuilder brokerConfig, String listeners, Properties props) {
        if (brokerConfig.nodeId >= 0) props.put(KafkaConfig.BROKER_ID_PROP, Integer.toString(brokerConfig.nodeId));
        props.put(KafkaConfig.LISTENERS_PROP, listeners);
        props.put(KafkaConfig.ZK_CONNECT_PROP, brokerConfig.zkConnect);
        props.put(KafkaConfig.ZK_CONNECTION_TIMEOUT_MS_PROP, "10000");
    }

    private static Properties setupBaseConfig(BrokerConfigBuilder brokerConfig) {
        Properties props = new Properties();
        props.put("unstable.metadata.versions.enable", "true");
        props.put(KafkaConfig.REPLICA_SOCKET_TIMEOUT_MS_PROP, "1500");
        props.put(KafkaConfig.CONTROLLER_SOCKET_TIMEOUT_MS_PROP, "1500");
        props.put(KafkaConfig.CONTROLLED_SHUTDOWN_ENABLE_PROP, Boolean.toString(brokerConfig.enableControlledShutdown));
        props.put(KafkaConfig.DELETE_TOPIC_ENABLE_PROP, Boolean.toString(brokerConfig.enableDeleteTopic));
        props.put(KafkaConfig.LOG_DELETE_DELAY_MS_PROP, "1000");
        props.put(KafkaConfig.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_PROP, "100");
        props.put(KafkaConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152");
        props.put(KafkaConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_PROP, "1");
        props.put(KafkaConfig.NUM_PARTITIONS_PROP, Integer.toString(brokerConfig.numPartitions));
        props.put(KafkaConfig.DEFAULT_REPLICATION_FACTOR_PROP, Short.toString(brokerConfig.defaultReplicationFactor));
        // Reduce number of threads per broker
        props.put(KafkaConfig.NUM_NETWORK_THREADS_PROP, "2");
        props.put(KafkaConfig.BACKGROUND_THREADS_PROP, "2");
        return props;
    }

    private static void setKRAFTModeConfig(BrokerConfigBuilder brokerConfig, Properties props, String listeners, List<Map.Entry<SecurityProtocol, Integer>> protocolAndPorts) {
        props.setProperty(KafkaConfig.SERVER_MAX_STARTUP_TIME_MS_PROP, Long.toString(TimeUnit.MINUTES.toMillis(10)));
        props.put(KafkaConfig.NODE_ID_PROP, Integer.toString(brokerConfig.nodeId));
        props.put(KafkaConfig.BROKER_ID_PROP, Integer.toString(brokerConfig.nodeId));
        props.put(KafkaConfig.ADVERTISED_LISTENERS_PROP, listeners);
        props.put(KafkaConfig.LISTENERS_PROP, listeners);
        props.put(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER");
        props.put(KafkaConfig.LISTENER_SECURITY_PROTOCOL_MAP_PROP, protocolAndPorts.stream()
                .map(entry -> entry.getKey().name() + ":" + entry.getKey().name())
                .reduce((s1, s2) -> s1 + "," + s2).orElse("") + ",CONTROLLER:PLAINTEXT");
        props.put(KafkaConfig.PROCESS_ROLES_PROP, "broker");
        props.put(KafkaConfig.QUORUM_VOTERS_PROP, "1000@localhost:0");
    }

    private static boolean shouldEnable(Optional<SecurityProtocol> interBrokerSecurityProtocol, SecurityProtocol protocol) {
        return interBrokerSecurityProtocol.isPresent() && interBrokerSecurityProtocol.get() == protocol;
    }

    /**
     * Create a temporary directory
     */
    public static File tempDir() {
        return org.apache.kafka.test.TestUtils.tempDirectory();
    }

    /**
     * Create a temporary relative directory
     */
    public static File tempRelativeDir(String parent) {
        File parentFile = new File(parent);
        parentFile.mkdirs();

        return org.apache.kafka.test.TestUtils.tempDirectory(parentFile.toPath(), null);
    }

    private static boolean usesSslTransportLayer(SecurityProtocol securityProtocol) {
        return securityProtocol == SecurityProtocol.SSL || securityProtocol == SecurityProtocol.SASL_SSL;
    }

    public static Properties sslConfigs(Mode mode, boolean clientCert, Optional<File> trustStoreFile, String certAlias,
                                        String certCn, String tlsProtocol) {
        File trustStore = trustStoreFile.orElseThrow(() ->
                new RuntimeException("SSL enabled but no trustStoreFile provided")
        );

        TestSslUtils.SslConfigsBuilder sslConfigsBuilder = new TestSslUtils.SslConfigsBuilder(mode)
                .useClientCert(clientCert)
                .createNewTrustStore(trustStore)
                .certAlias(certAlias)
                .cn(certCn)
                .tlsProtocol(tlsProtocol);

        Properties sslProps = new Properties();
        try {
            sslConfigsBuilder.build().forEach(sslProps::put);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return sslProps;
    }

    static boolean usesSaslAuthentication(SecurityProtocol securityProtocol) {
        return securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL;
    }

    static public class BrokerConfigBuilder {
        private final int nodeId;
        private final String zkConnect;
        private final boolean enableControlledShutdown;
        private final boolean enableDeleteTopic;
        private final int port;
        private final Optional<SecurityProtocol> interBrokerSecurityProtocol;
        private final Optional<File> trustStoreFile;
        private final Optional<Properties> saslProperties;
        private final boolean enablePlaintext;
        private final boolean enableSaslPlaintext;
        private final int saslPlaintextPort;
        private final boolean enableSsl;
        private final int sslPort;
        private final boolean enableSaslSsl;
        private final int saslSslPort;
        private final Optional<String> rack;
        private final int logDirCount;
        private final boolean enableToken;
        private final int numPartitions;
        private final short defaultReplicationFactor;
        private final boolean enableFetchFromFollower;

        private BrokerConfigBuilder(Builder builder) {
            this.nodeId = builder.nodeId;
            this.zkConnect = builder.zkConnect;
            this.enableControlledShutdown = builder.enableControlledShutdown;
            this.enableDeleteTopic = builder.enableDeleteTopic;
            this.port = builder.port;
            this.interBrokerSecurityProtocol = builder.interBrokerSecurityProtocol;
            this.trustStoreFile = builder.trustStoreFile;
            this.saslProperties = builder.saslProperties;
            this.enablePlaintext = builder.enablePlaintext;
            this.enableSaslPlaintext = builder.enableSaslPlaintext;
            this.saslPlaintextPort = builder.saslPlaintextPort;
            this.enableSsl = builder.enableSsl;
            this.sslPort = builder.sslPort;
            this.enableSaslSsl = builder.enableSaslSsl;
            this.saslSslPort = builder.saslSslPort;
            this.rack = builder.rack;
            this.logDirCount = builder.logDirCount;
            this.enableToken = builder.enableToken;
            this.numPartitions = builder.numPartitions;
            this.defaultReplicationFactor = builder.defaultReplicationFactor;
            this.enableFetchFromFollower = builder.enableFetchFromFollower;
        }
        static public class Builder {
            private final int nodeId;
            private final String zkConnect;
            private boolean enableControlledShutdown = true;
            private boolean enableDeleteTopic = true;
            private int port = RANDOM_PORT;
            private Optional<SecurityProtocol> interBrokerSecurityProtocol = Optional.empty();
            private Optional<File> trustStoreFile = Optional.empty();
            private Optional<Properties> saslProperties = Optional.empty();
            private boolean enablePlaintext = true;
            private boolean enableSaslPlaintext = false;
            private int saslPlaintextPort = RANDOM_PORT;
            private boolean enableSsl = false;
            private int sslPort = RANDOM_PORT;
            private boolean enableSaslSsl = false;
            private int saslSslPort = RANDOM_PORT;
            private Optional<String> rack = Optional.empty();
            private int logDirCount = 1;
            private boolean enableToken = false;
            private int numPartitions = 1;
            private short defaultReplicationFactor = (short) 1;
            private boolean enableFetchFromFollower = false;

            public Builder(int nodeId, String zkConnect) {
                this.nodeId = nodeId;
                this.zkConnect = zkConnect;
            }

            public Builder enableControlledShutdown(boolean enableControlledShutdown) {
                this.enableControlledShutdown = enableControlledShutdown;
                return this;
            }

            public Builder enableDeleteTopic(boolean enableDeleteTopic) {
                this.enableDeleteTopic = enableDeleteTopic;
                return this;
            }

            public Builder port(int port) {
                this.port = port;
                return this;
            }

            public Builder interBrokerSecurityProtocol(Optional<SecurityProtocol> interBrokerSecurityProtocol) {
                this.interBrokerSecurityProtocol = interBrokerSecurityProtocol;
                return this;
            }

            public Builder trustStoreFile(Optional<File> trustStoreFile) {
                this.trustStoreFile = trustStoreFile;
                return this;
            }

            public Builder saslProperties(Optional<Properties> saslProperties) {
                this.saslProperties = saslProperties;
                return this;
            }

            public Builder enablePlaintext(Boolean enablePlaintext) {
                this.enablePlaintext = enablePlaintext;
                return this;
            }

            public Builder enableSaslPlaintext(Boolean enableSaslPlaintext) {
                this.enableSaslPlaintext = enableSaslPlaintext;
                return this;
            }

            public Builder saslPlaintextPort(int saslPlaintextPort) {
                this.saslPlaintextPort = saslPlaintextPort;
                return this;
            }

            public Builder enableSsl(Boolean enableSsl) {
                this.enableSsl = enableSsl;
                return this;
            }

            public Builder sslPort(int sslPort) {
                this.sslPort = sslPort;
                return this;
            }

            public Builder enableSaslSsl(Boolean enableSaslSsl) {
                this.enableSaslSsl = enableSaslSsl;
                return this;
            }

            public Builder saslSslPort(int saslSslPort) {
                this.saslSslPort = saslSslPort;
                return this;
            }

            public Builder rack(Optional<String> rack) {
                this.rack = rack;
                return this;
            }

            public Builder logDirCount(int logDirCount) {
                this.logDirCount = logDirCount;
                return this;
            }

            public Builder enableToken(Boolean enableToken) {
                this.enableToken = enableToken;
                return this;
            }

            public Builder numPartitions(int numPartitions) {
                this.numPartitions = numPartitions;
                return this;
            }

            public Builder defaultReplicationFactor(Short defaultReplicationFactor) {
                this.defaultReplicationFactor = defaultReplicationFactor;
                return this;
            }

            public Builder enableFetchFromFollower(Boolean enableFetchFromFollower) {
                this.enableFetchFromFollower = enableFetchFromFollower;
                return this;
            }
            public BrokerConfigBuilder build() {
                return new BrokerConfigBuilder(this);
            }
        }
    }
}