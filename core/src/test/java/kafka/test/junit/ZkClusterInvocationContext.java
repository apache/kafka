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

import integration.kafka.server.IntegrationTestHelper;
import kafka.api.IntegrationTestHarness;
import kafka.network.SocketServer;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
        return String.format("[Zk %d] %s", invocationIndex, clusterDesc);
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
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
                    public SecurityProtocol securityProtocol() {
                        return SecurityProtocol.forName(clusterConfig.securityProtocol());
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
                        // Brokers and controllers are the same in zk mode, so just use the max
                        return Math.max(clusterConfig.brokers(), clusterConfig.controllers());
                    }
                };
                cluster.adminClientConfig().putAll(clusterConfig.adminClientProperties());
                cluster.serverConfig().putAll(clusterConfig.serverProperties());
                // TODO consumer and producer configs
                clusterReference.set(cluster);
                if (clusterConfig.isAutoStart()) {
                    clusterShim.start();
                }
            },
            (AfterTestExecutionCallback) context -> clusterShim.stop(),
            new ClusterInstanceParameterResolver(clusterShim),
            new GenericParameterResolver<>(clusterConfig, ClusterConfig.class),
            new GenericParameterResolver<>(new IntegrationTestHelper(), IntegrationTestHelper.class)
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
        public String brokerList() {
            return TestUtils.bootstrapServers(clusterReference.get().servers(), clusterReference.get().listenerName());
        }

        @Override
        public Collection<SocketServer> brokers() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                    .map(KafkaServer::socketServer)
                    .collect(Collectors.toList());
        }

        @Override
        public ListenerName listener() {
            return clusterReference.get().listenerName();
        }

        @Override
        public Collection<SocketServer> controllers() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                .filter(broker -> broker.kafkaController().isActive())
                .map(KafkaServer::socketServer)
                .collect(Collectors.toList());
        }

        @Override
        public Optional<SocketServer> anyBroker() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                .map(KafkaServer::socketServer)
                .findFirst();
        }

        @Override
        public Optional<SocketServer> anyController() {
            return JavaConverters.asJavaCollection(clusterReference.get().servers()).stream()
                .filter(broker -> broker.kafkaController().isActive())
                .map(KafkaServer::socketServer)
                .findFirst();
        }

        @Override
        public ClusterType clusterType() {
            return ClusterType.Zk;
        }

        @Override
        public ClusterConfig config() {
            return config;
        }

        @Override
        public Object getUnderlying() {
            return clusterReference.get();
        }

        @Override
        public Admin createAdminClient(Properties configOverrides) {
            return clusterReference.get().createAdminClient(configOverrides);
        }

        @Override
        public void start() {
            if (started.compareAndSet(false, true)) {
                clusterReference.get().setUp();
            }
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                clusterReference.get().tearDown();
            }
        }
    }
}
