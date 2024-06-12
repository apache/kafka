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
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;
import kafka.server.KafkaServer;
import kafka.test.annotation.Type;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.utils.EmptyTestInfo;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.server.config.ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG;

/**
 * Wraps a {@link IntegrationTestHarness} inside lifecycle methods for a test invocation. Each instance of this
 * class is provided with a configuration for the cluster.
 *
 * This context also provides parameter resolvers for:
 *
 * <ul>
 *     <li>ClusterConfig (the same instance passed to the constructor)</li>
 *     <li>ClusterInstance (includes methods to expose underlying SocketServer-s)</li>
 * </ul>
 */
public class ZkClusterInvocationContext implements TestTemplateInvocationContext {

    private final String baseDisplayName;
    private final ClusterConfig clusterConfig;
    private final AtomicReference<ClusterConfigurableIntegrationHarness> clusterReference;

    public ZkClusterInvocationContext(String baseDisplayName, ClusterConfig clusterConfig) {
        this.baseDisplayName = baseDisplayName;
        this.clusterConfig = clusterConfig;
        this.clusterReference = new AtomicReference<>();
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        return String.format("%s [%d] Type=ZK, %s", baseDisplayName, invocationIndex, String.join(",", clusterConfig.displayTags()));
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
                clusterReference.set(new ClusterConfigurableIntegrationHarness(clusterConfig));
                if (clusterConfig.isAutoStart()) {
                    clusterShim.start();
                }
            },
            (AfterTestExecutionCallback) context -> clusterShim.stop(),
            new ClusterInstanceParameterResolver(clusterShim)
        );
    }

    public static class ZkClusterInstance implements ClusterInstance {

        final AtomicReference<ClusterConfigurableIntegrationHarness> clusterReference;
        final ClusterConfig config;
        final AtomicBoolean started = new AtomicBoolean(false);
        final AtomicBoolean stopped = new AtomicBoolean(false);

        ZkClusterInstance(ClusterConfig config, AtomicReference<ClusterConfigurableIntegrationHarness> clusterReference) {
            this.config = config;
            this.clusterReference = clusterReference;
        }

        @Override
        public String bootstrapServers() {
            return TestUtils.bootstrapServers(clusterReference.get().servers(), clusterReference.get().listenerName());
        }

        @Override
        public String bootstrapControllers() {
            throw new RuntimeException("Cannot use --bootstrap-controller with ZK-based clusters.");
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
            return brokers().values().stream()
                .filter(s -> ((KafkaServer) s).kafkaController().isActive())
                .map(KafkaBroker::socketServer)
                .collect(Collectors.toList());
        }

        @Override
        public String clusterId() {
            return brokers().values().stream().findFirst().map(KafkaBroker::clusterId).orElseThrow(
                () -> new RuntimeException("No broker instances found"));
        }

        @Override
        public Type type() {
            return Type.ZK;
        }

        @Override
        public ClusterConfig config() {
            return config;
        }

        @Override
        public Set<Integer> controllerIds() {
            return brokerIds();
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

        /**
         * Restart brokers with given cluster config.
         *
         * @param clusterConfig clusterConfig is optional. If left Optional.empty(), brokers will restart without
         *                      reconfiguring configurations. Otherwise, the restart will reconfigure configurations
         *                      according to the provided cluster config.
         */
        public void rollingBrokerRestart(Optional<ClusterConfig> clusterConfig) {
            requireNonNull(clusterConfig);
            if (!started.get()) {
                throw new IllegalStateException("Tried to restart brokers but the cluster has not been started!");
            }
            for (int i = 0; i < clusterReference.get().brokerCount(); i++) {
                clusterReference.get().killBroker(i);
            }
            clusterConfig.ifPresent(config -> clusterReference.get().setClusterConfig(config));
            clusterReference.get().restartDeadBrokers(true);
            clusterReference.get().adminClientConfig().put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        }

        @Override
        public void waitForReadyBrokers() throws InterruptedException {
            org.apache.kafka.test.TestUtils.waitForCondition(() -> {
                int numRegisteredBrokers = clusterReference.get().zkClient().getAllBrokersInCluster().size();
                return numRegisteredBrokers == config.numBrokers();
            }, "Timed out while waiting for brokers to become ready");
        }

        private KafkaServer findBrokerOrThrow(int brokerId) {
            return brokers().values().stream()
                .filter(server -> server.config().brokerId() == brokerId)
                .map(s -> (KafkaServer) s)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown brokerId " + brokerId));
        }

        @Override
        public Map<Integer, ControllerServer> controllers() {
            return Collections.emptyMap();
        }

        @Override
        public Map<Integer, KafkaBroker> brokers() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers())
                    .stream().collect(Collectors.toMap(s -> s.config().brokerId(), s -> s));
        }
    }

    // This is what tests normally extend from to start a cluster, here we extend it and
    // configure the cluster using values from ClusterConfig.
    private static class ClusterConfigurableIntegrationHarness extends IntegrationTestHarness {
        private ClusterConfig clusterConfig;

        private ClusterConfigurableIntegrationHarness(ClusterConfig clusterConfig) {
            this.clusterConfig = Objects.requireNonNull(clusterConfig);
        }

        public void setClusterConfig(ClusterConfig clusterConfig) {
            this.clusterConfig = Objects.requireNonNull(clusterConfig);
        }

        @Override
        public void modifyConfigs(Seq<Properties> props) {
            super.modifyConfigs(props);
            for (int i = 0; i < props.length(); i++) {
                props.apply(i).putAll(clusterConfig.perServerOverrideProperties().getOrDefault(i, Collections.emptyMap()));
            }
        }

        @Override
        public Properties serverConfig() {
            Properties props = new Properties();
            props.putAll(clusterConfig.serverProperties());
            props.put(INTER_BROKER_PROTOCOL_VERSION_CONFIG, clusterConfig.metadataVersion().version());
            return props;
        }

        @Override
        public Properties adminClientConfig() {
            Properties props = new Properties();
            props.putAll(clusterConfig.adminClientProperties());
            return props;
        }

        @Override
        public Properties consumerConfig() {
            Properties props = new Properties();
            props.putAll(clusterConfig.consumerProperties());
            return props;
        }

        @Override
        public Properties producerConfig() {
            Properties props = new Properties();
            props.putAll(clusterConfig.producerProperties());
            return props;
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
                Properties props = new Properties();
                props.putAll(clusterConfig.saslServerProperties());
                return Option.apply(props);
            }
        }

        @Override
        public Option<Properties> clientSaslProperties() {
            if (clusterConfig.saslClientProperties().isEmpty()) {
                return Option.empty();
            } else {
                Properties props = new Properties();
                props.putAll(clusterConfig.saslClientProperties());
                return Option.apply(props);
            }
        }

        @Override
        public int brokerCount() {
            // Controllers are also brokers in zk mode, so just use broker count
            return clusterConfig.numBrokers();
        }

        @Override
        public int logDirCount() {
            return clusterConfig.numDisksPerBroker();
        }

        @Override
        public Option<File> trustStoreFile() {
            return OptionConverters.toScala(clusterConfig.trustStoreFile());
        }
    }
}
