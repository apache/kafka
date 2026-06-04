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
package org.apache.kafka.tiered.storage;

import kafka.integration.KafkaServerTestHarness;
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.common.network.ConnectionMode;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.fault.FaultHandlerException;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import scala.jdk.javaapi.CollectionConverters;

/**
 * A {@link ClusterInstance} implementation backed by a {@link KafkaServerTestHarness}.
 * This allows {@link TieredStorageTestContext} to depend only on {@link ClusterInstance}
 * rather than on the concrete harness class.
 */
public class HarnessBackedClusterInstance implements ClusterInstance {

    private final KafkaServerTestHarness harness;

    public HarnessBackedClusterInstance(KafkaServerTestHarness harness) {
        this.harness = harness;
    }

    @Override
    public Type type() {
        return Type.KRAFT;
    }

    @Override
    public ClusterConfig config() {
        return ClusterConfig.defaultBuilder()
                .setBrokers(harness.brokers().size())
                .setControllers(harness.controllerServers().size())
                .setBrokerSecurityProtocol(harness.securityProtocol())
                .setBrokerListenerName(harness.listenerName())
                .setControllerListenerName(controllerListenerName())
                .setMetadataVersion(harness.metadataVersion())
                .build();
    }

    @Override
    public Set<Integer> controllerIds() {
        return controllers().keySet();
    }

    @Override
    public ListenerName clientListener() {
        return harness.listenerName();
    }

    @Override
    public ListenerName controllerListenerName() {
        return controllers().values().stream()
                .map(c -> new ListenerName(c.config().controllerListenerNames().get(0)))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No controllers available"));
    }

    @Override
    public String bootstrapServers() {
        return harness.bootstrapServers(harness.listenerName());
    }

    @Override
    public String bootstrapControllers() {
        throw new UnsupportedOperationException("bootstrapControllers() is not supported in HarnessBackedClusterInstance");
    }

    @Override
    public String clusterId() {
        return brokers().values().stream()
                .map(KafkaBroker::clusterId)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No brokers available"));
    }

    @Override
    public Map<Integer, KafkaBroker> brokers() {
        return CollectionConverters.asJava(harness.brokers()).stream()
                .collect(Collectors.toMap(b -> b.config().brokerId(), b -> b));
    }

    @Override
    public Map<Integer, ControllerServer> controllers() {
        return CollectionConverters.asJava(harness.controllerServers()).stream()
                .collect(Collectors.toMap(c -> c.config().nodeId(), c -> c));
    }

    @Override
    public void shutdownBroker(int brokerId) {
        harness.killBroker(brokerId);
    }

    @Override
    public void startBroker(int brokerId) {
        harness.startBroker(brokerId);
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException("start() is managed by KafkaServerTestHarness");
    }

    @Override
    public boolean started() {
        return true;
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("stop() is managed by KafkaServerTestHarness");
    }

    @Override
    public boolean stopped() {
        return false;
    }

    @Override
    public void waitForReadyBrokers() throws InterruptedException {
        Map<Integer, KafkaBroker> brokerMap = brokers();

        // Step 1: wait until a controller marks all brokers as registered and unfenced
        ControllerServer controllerServer = controllers().values().iterator().next();
        try {
            controllerServer.controller().waitForReadyBrokers(brokerMap.size()).get();
        } catch (ExecutionException e) {
            throw new AssertionError("Failed while waiting for brokers to become ready", e);
        }

        // Step 2: wait until each broker's metadata cache knows about all alive brokers
        Set<Integer> brokerIds = brokerMap.keySet();
        TestUtils.waitForCondition(
            () -> brokerMap.values().stream().allMatch(
                broker -> brokerIds.stream().allMatch(id -> broker.metadataCache().hasAliveBroker(id))
            ),
            "Timed out waiting for metadata cache to reflect all alive brokers"
        );
    }

    @Override
    public Optional<FaultHandlerException> firstFatalException() {
        return Optional.ofNullable(harness.faultHandler().firstException());
    }

    @Override
    public Optional<FaultHandlerException> firstNonFatalException() {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> setClientSslConfig(Map<String, Object> configs) {
        if (harness.trustStoreFile().isEmpty()) {
            return configs;
        }
        try {
            Map<String, Object> props = new HashMap<>(configs);
            props.putAll(new TestSslUtils.SslConfigsBuilder(ConnectionMode.CLIENT)
                    .useExistingTrustStore(harness.trustStoreFile().get())
                    .build());
            return props;
        } catch (Exception e) {
            throw new RuntimeException("Failed to build client SSL config", e);
        }
    }
}
