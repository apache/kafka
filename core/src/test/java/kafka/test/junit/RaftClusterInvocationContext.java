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

import kafka.network.SocketServer;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.testkit.KafkaClusterTestKit;
import kafka.testkit.TestKitNodes;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerState;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import scala.compat.java8.OptionConverters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Wraps a {@link KafkaClusterTestKit} inside lifecycle methods for a test invocation. Each instance of this
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
public class RaftClusterInvocationContext implements TestTemplateInvocationContext {

    private final ClusterConfig clusterConfig;
    private final AtomicReference<KafkaClusterTestKit> clusterReference;

    public RaftClusterInvocationContext(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
        this.clusterReference = new AtomicReference<>();
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        String clusterDesc = clusterConfig.nameTags().entrySet().stream()
                .map(Object::toString)
                .collect(Collectors.joining(", "));
        return String.format("[%d] Type=Raft, %s", invocationIndex, clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        RaftClusterInstance clusterInstance = new RaftClusterInstance(clusterReference, clusterConfig);
        return Arrays.asList(
            (BeforeTestExecutionCallback) context -> {
                TestKitNodes nodes = new TestKitNodes.Builder().
                        setNumBrokerNodes(clusterConfig.numBrokers()).
                        setNumControllerNodes(clusterConfig.numControllers()).build();
                nodes.brokerNodes().forEach((brokerId, brokerNode) -> {
                    clusterConfig.brokerServerProperties(brokerId).forEach(
                            (key, value) -> brokerNode.propertyOverrides().put(key.toString(), value.toString()));
                });
                KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);

                // Copy properties into the TestKit builder
                clusterConfig.serverProperties().forEach((key, value) -> builder.setConfigProp(key.toString(), value.toString()));
                // KAFKA-12512 need to pass security protocol and listener name here
                KafkaClusterTestKit cluster = builder.build();
                clusterReference.set(cluster);
                cluster.format();
                cluster.startup();
                kafka.utils.TestUtils.waitUntilTrue(
                    () -> cluster.brokers().get(0).brokerState() == BrokerState.RUNNING,
                    () -> "Broker never made it to RUNNING state.",
                    org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS,
                    100L);
            },
            (AfterTestExecutionCallback) context -> clusterInstance.stop(),
            new ClusterInstanceParameterResolver(clusterInstance),
            new GenericParameterResolver<>(clusterConfig, ClusterConfig.class)
        );
    }

    public static class RaftClusterInstance implements ClusterInstance {

        private final AtomicReference<KafkaClusterTestKit> clusterReference;
        private final ClusterConfig clusterConfig;
        final AtomicBoolean started = new AtomicBoolean(false);
        final AtomicBoolean stopped = new AtomicBoolean(false);
        private final ConcurrentLinkedQueue<Admin> admins = new ConcurrentLinkedQueue<>();

        RaftClusterInstance(AtomicReference<KafkaClusterTestKit> clusterReference, ClusterConfig clusterConfig) {
            this.clusterReference = clusterReference;
            this.clusterConfig = clusterConfig;
        }

        @Override
        public String bootstrapServers() {
            return clusterReference.get().clientProperties().getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        }

        @Override
        public Collection<SocketServer> brokerSocketServers() {
            return brokers()
                .map(BrokerServer::socketServer)
                .collect(Collectors.toList());
        }

        @Override
        public ListenerName clientListener() {
            return ListenerName.normalised("EXTERNAL");
        }

        @Override
        public Optional<ListenerName> controllerListenerName() {
            return OptionConverters.toJava(controllers().findAny().get().config().controllerListenerNames().headOption().map(ListenerName::new));
        }

        @Override
        public Collection<SocketServer> controllerSocketServers() {
            return controllers()
                .map(ControllerServer::socketServer)
                .collect(Collectors.toList());
        }

        @Override
        public SocketServer anyBrokerSocketServer() {
            return brokers()
                .map(BrokerServer::socketServer)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No broker SocketServers found"));
        }

        @Override
        public SocketServer anyControllerSocketServer() {
            return controllers()
                .map(ControllerServer::socketServer)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No controller SocketServers found"));
        }

        @Override
        public ClusterType clusterType() {
            return ClusterType.RAFT;
        }

        @Override
        public ClusterConfig config() {
            return clusterConfig;
        }

        @Override
        public KafkaClusterTestKit getUnderlying() {
            return clusterReference.get();
        }

        @Override
        public Admin createAdminClient(Properties configOverrides) {
            Admin admin = Admin.create(clusterReference.get().clientProperties());
            admins.add(admin);
            return admin;
        }

        @Override
        public void start() {
            if (started.compareAndSet(false, true)) {
                try {
                    clusterReference.get().startup();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to start Raft server", e);
                }
            }
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                admins.forEach(admin -> Utils.closeQuietly(admin, "admin"));
                Utils.closeQuietly(clusterReference.get(), "cluster");
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
        public void waitForReadyBrokers() throws InterruptedException {
            try {
                clusterReference.get().waitForReadyBrokers();
            } catch (ExecutionException e) {
                throw new AssertionError("Failed while waiting for brokers to become ready", e);
            }
        }

        @Override
        public void rollingBrokerRestart() {
            throw new UnsupportedOperationException("Restarting Raft servers is not yet supported.");
        }

        private BrokerServer findBrokerOrThrow(int brokerId) {
            return Optional.ofNullable(clusterReference.get().brokers().get(brokerId))
                .orElseThrow(() -> new IllegalArgumentException("Unknown brokerId " + brokerId));
        }

        private Stream<BrokerServer> brokers() {
            return clusterReference.get().brokers().values().stream();
        }

        private Stream<ControllerServer> controllers() {
            return clusterReference.get().controllers().values().stream();
        }

    }
}
