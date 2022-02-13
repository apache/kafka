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
import kafka.server.KafkaServer;
import kafka.server.MetadataCache;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.utils.EmptyTestInfo;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;
import scala.collection.JavaConverters;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZkClusterInstance implements ClusterInstance {

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
    public Map<Integer, SocketServer> brokerSocketServers() {
        return servers().collect(Collectors.toMap(
            server -> server.config().nodeId(),
            KafkaServer::socketServer
        ));
    }

    @Override
    public ListenerName clientListener() {
        return clusterReference.get().listenerName();
    }

    @Override
    public Map<Integer, SocketServer> controllerSocketServers() {
        return servers()
            .filter(broker -> broker.kafkaController().isActive())
            .collect(Collectors.toMap(
                server -> server.config().nodeId(),
                KafkaServer::socketServer
            ));
    }

    @Override
    public Map<Integer, MetadataCache> brokerMetadataCaches() {
        return servers().collect(Collectors.toMap(
            server -> server.config().nodeId(),
            KafkaServer::metadataCache
        ));
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
        return clusterReference.get().createAdminClient(configOverrides);
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
