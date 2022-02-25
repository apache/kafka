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

package kafka.test.junit;

import kafka.api.IntegrationTestHarness;
import kafka.network.SocketServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.utils.EmptyTestInfo;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.compat.java8.OptionConverters;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wraps a {@link IntegrationTestHarness} inside lifecycle methods for a test invocation. Each instance of this
 * class is provided with a configuration for the cluster.
 *
 * This context also provides parameter resolvers for:
 *
 * <ul>
 *     <li>ClusterConfig (the same instance passed to the constructor)</li>
 *     <li>ClusterInstance (includes methods to expose underlying SocketServer-s)</li>
 *     <li>IntegrationTestHelper (helper methods)</li>
 * </ul>
 */
public class ZkClusterInvocationContext implements TestTemplateInvocationContext {

    private final ClusterConfig clusterConfig;
    private final AtomicReference<IntegrationTestHarness> clusterReference;

    public ZkClusterInvocationContext(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.clusterReference = new AtomicReference<>();
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        String clusterDesc = clusterConfig.nameTags().entrySet().stream()
            .map(Object::toString)
            .collect(Collectors.joining(", "));
        return String.format("[%d] Type=ZK, %s", invocationIndex, clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        if (clusterConfig.numControllers() != 1) {
            throw new IllegalArgumentException("For ZK clusters, please specify exactly 1 controller.");
        }
        ClusterInstance clusterShim = new ZkClusterInstance(clusterConfig, clusterReference);
        return Arrays.asList(
            (BeforeTestExecutionCallback) context -> {
                // We have to wait to actually create the underlying cluster until after our @BeforeEach methods
                // have run. This allows tests to set up external dependencies like ZK, MiniKDC, etc.
                // However, since we cannot create this instance until we are inside the test invocation, we have
                // to use a container class (AtomicReference) to provide this cluster object to the test itself

                // This is what tests normally extend from to start a cluster, here we create it anonymously and
                // configure the cluster using values from ClusterConfig
                IntegrationTestHarness cluster = new IntegrationTestHarness() {

                    @Override
                    public void modifyConfigs(Seq<Properties> props) {
                        super.modifyConfigs(props);
                        for (int i = 0; i < props.length(); i++) {
                            props.apply(i).putAll(clusterConfig.brokerServerProperties(i));
                        }
                    }

                    @Override
                    public Properties serverConfig() {
                        Properties props = clusterConfig.serverProperties();
                        clusterConfig.ibp().ifPresent(ibp -> props.put(KafkaConfig.InterBrokerProtocolVersionProp(), ibp));
                        return props;
                    }

                    @Override
                    public Properties adminClientConfig() {
                        return clusterConfig.adminClientProperties();
                    }

                    @Override
                    public Properties consumerConfig() {
                        return clusterConfig.consumerProperties();
                    }

                    @Override
                    public Properties producerConfig() {
                        return clusterConfig.producerProperties();
                    }

                    @Override
                    public SecurityProtocol securityProtocol() {
                        return clusterConfig.securityProtocol();
                    }

                    @Override
                    public ListenerName listenerName() {
                        return clusterConfig.listenerName().map(ListenerName::normalised)
                            .orElseGet(() -> ListenerName.forSecurityProtocol(securityProtocol()));
                    }

                    @Override
                    public Option<Properties> serverSaslProperties() {
                        if (clusterConfig.saslServerProperties().isEmpty()) {
                            return Option.empty();
                        } else {
                            return Option.apply(clusterConfig.saslServerProperties());
                        }
                    }

                    @Override
                    public Option<Properties> clientSaslProperties() {
                        if (clusterConfig.saslClientProperties().isEmpty()) {
                            return Option.empty();
                        } else {
                            return Option.apply(clusterConfig.saslClientProperties());
                        }
                    }

                    @Override
                    public int brokerCount() {
                        // Controllers are also brokers in zk mode, so just use broker count
                        return clusterConfig.numBrokers();
                    }

                    @Override
                    public Option<File> trustStoreFile() {
                        return OptionConverters.toScala(clusterConfig.trustStoreFile());
                    }
                };

                clusterReference.set(cluster);
                if (clusterConfig.isAutoStart()) {
                    clusterShim.start();
                }
            },
            (AfterTestExecutionCallback) context -> clusterShim.stop(),
            new ClusterInstanceParameterResolver(clusterShim),
            new GenericParameterResolver<>(clusterConfig, ClusterConfig.class)
        );
    }

    public static class ZkClusterInstance implements ClusterInstance {

        final AtomicReference<IntegrationTestHarness> clusterReference;
        final ClusterConfig config;
        final AtomicBoolean started = new AtomicBoolean(false);
        final AtomicBoolean stopped = new AtomicBoolean(false);

        ZkClusterInstance(ClusterConfig config, AtomicReference<IntegrationTestHarness> clusterReference) {
            this.config = config;
            this.clusterReference = clusterReference;
        }

        @Override
        public String bootstrapServers() {
            return TestUtils.bootstrapServers(clusterReference.get().servers(), clusterReference.get().listenerName());
        }

        @Override
        public Collection<SocketServer> brokerSocketServers() {
            return servers()
                    .map(KafkaServer::socketServer)
                    .collect(Collectors.toList());
        }

        @Override
        public ListenerName clientListener() {
            return clusterReference.get().listenerName();
        }


        @Override
        public Optional<ListenerName> controlPlaneListenerName() {
            return OptionConverters.toJava(clusterReference.get().servers().head().config().controlPlaneListenerName());
        }

        @Override
        public Collection<SocketServer> controllerSocketServers() {
            return servers()
                .filter(broker -> broker.kafkaController().isActive())
                .map(KafkaServer::socketServer)
                .collect(Collectors.toList());
        }

        @Override
        public SocketServer anyBrokerSocketServer() {
            return servers()
                .map(KafkaServer::socketServer)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
        }

        @Override
        public SocketServer anyControllerSocketServer() {
            return servers()
                .filter(broker -> broker.kafkaController().isActive())
                .map(KafkaServer::socketServer)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
        }

        @Override
        public ClusterType clusterType() {
            return ClusterType.ZK;
        }

        @Override
        public ClusterConfig config() {
            return config;
        }

        @Override
        public IntegrationTestHarness getUnderlying() {
            return clusterReference.get();
        }

        @Override
        public Admin createAdminClient(Properties configOverrides) {
            return clusterReference.get().createAdminClient(clientListener(), configOverrides);
        }

        @Override
        public void start() {
            if (started.compareAndSet(false, true)) {
                clusterReference.get().setUp(new EmptyTestInfo());
            }
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                clusterReference.get().tearDown();
            }
        }

        @Override
        public void shutdownBroker(int brokerId) {
            findBrokerOrThrow(brokerId).shutdown();
        }

        @Override
        public void startBroker(int brokerId) {
            findBrokerOrThrow(brokerId).startup();
        }

        @Override
        public void rollingBrokerRestart() {
            if (!started.get()) {
                throw new IllegalStateException("Tried to restart brokers but the cluster has not been started!");
            }
            for (int i = 0; i < clusterReference.get().brokerCount(); i++) {
                clusterReference.get().killBroker(i);
            }
            clusterReference.get().restartDeadBrokers(true);
        }

        @Override
        public void waitForReadyBrokers() throws InterruptedException {
            org.apache.kafka.test.TestUtils.waitForCondition(() -> {
                int numRegisteredBrokers = clusterReference.get().zkClient().getAllBrokersInCluster().size();
                return numRegisteredBrokers == config.numBrokers();
            }, "Timed out while waiting for brokers to become ready");
        }

        private KafkaServer findBrokerOrThrow(int brokerId) {
            return servers()
                .filter(server -> server.config().brokerId() == brokerId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown brokerId " + brokerId));
        }

        private Stream<KafkaServer> servers() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream();
        }

    }
}
