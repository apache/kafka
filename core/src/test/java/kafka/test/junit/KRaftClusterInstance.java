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
import kafka.server.MetadataCache;
import kafka.test.ClusterConfig;
import kafka.test.ClusterInstance;
import kafka.testkit.KafkaClusterTestKit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.image.MetadataImage;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KRaftClusterInstance implements ClusterInstance {

    private final AtomicReference<KafkaClusterTestKit> clusterReference;
    private final ClusterConfig clusterConfig;
    final AtomicBoolean started = new AtomicBoolean(false);
    final AtomicBoolean stopped = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<Admin> admins = new ConcurrentLinkedQueue<>();

    KRaftClusterInstance(AtomicReference<KafkaClusterTestKit> clusterReference, ClusterConfig clusterConfig) {
        this.clusterReference = clusterReference;
        this.clusterConfig = clusterConfig;
    }

    @Override
    public String bootstrapServers() {
        return clusterReference.get().clientProperties().getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    }

    @Override
    public Map<Integer, SocketServer> brokerSocketServers() {
        return brokers().collect(Collectors.toMap(
                brokerServer -> brokerServer.config().nodeId(),
                BrokerServer::socketServer
        ));
    }

    @Override
    public ListenerName clientListener() {
        return ListenerName.normalised("EXTERNAL");
    }

    @Override
    public Map<Integer, SocketServer> controllerSocketServers() {
        return controllers().collect(Collectors.toMap(
            controllerServer -> controllerServer.config().nodeId(),
            ControllerServer::socketServer
        ));
    }

    @Override
    public Map<Integer, MetadataCache> brokerMetadataCaches() {
        return brokers().collect(Collectors.toMap(
            brokerServer -> brokerServer.config().nodeId(),
            BrokerServer::metadataCache
        ));
    }

    public Map<Integer, MetadataImage> brokerCurrentMetadataImages() {
        return brokers().collect(Collectors.toMap(
            brokerServer -> brokerServer.config().nodeId(),
            brokerServer -> brokerServer.metadataCache().currentImage()
        ));
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
        return ClusterType.KRAFT;
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
                throw new RuntimeException("Failed to start KRaft server", e);
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
        throw new UnsupportedOperationException("Restarting KRaft servers is not yet supported.");
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
