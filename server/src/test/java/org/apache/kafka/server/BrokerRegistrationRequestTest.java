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
package org.apache.kafka.server;


import kafka.server.ControllerServer;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.BrokerRegistrationResponse;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionTestUtils;
import org.apache.kafka.server.common.NodeToControllerChannelManager;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test simulates a broker registering with the KRaft quorum under different configurations.
 */
class BrokerRegistrationRequestTest {
    private final ClusterInstance clusterInstance;
    public BrokerRegistrationRequestTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }
    
    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRejectZkMigratingBrokerWhenFeatureLevelDoesNotSupportMigration() throws Exception {
        try (ChannelEnv env = openChannel()) {
            assertEquals(
                    Errors.BROKER_ID_NOT_REGISTERED, 
                    registerBroker(env.channelManager, 1L, 
                            new BrokerRegistrationRequestData.Feature()
                                    .setName(MetadataVersion.FEATURE_NAME)
                                    .setMinSupportedVersion(MetadataVersionTestUtils.IBP_3_3_IV0_FEATURE_LEVEL)
                                    .setMaxSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel()))
            );
        }
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRejectRegistrationWithoutFeatureLevels() throws Exception {
        try (ChannelEnv env = openChannel()) {
            assertEquals(
                    Errors.INVALID_REGISTRATION,
                    registerBroker(env.channelManager, null, null)
            );
        }
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRejectRegistrationWhenFeatureLevelTooHigh() throws Exception {
        try (ChannelEnv env = openChannel()) {
            assertEquals(
                    Errors.UNSUPPORTED_VERSION,
                    registerBroker(env.channelManager, null,
                            new BrokerRegistrationRequestData.Feature()
                                    .setName(MetadataVersion.FEATURE_NAME)
                                    .setMinSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel())
                                    .setMaxSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel()))
            );
        }
    }

    @ClusterTest(types = {Type.KRAFT}, controllers = 1, metadataVersion = MetadataVersion.IBP_3_3_IV3)
    public void shouldRegisterWhenSupportedRangeAndNotMigrating() throws Exception {
        try (ChannelEnv env = openChannel()) {
            assertEquals(
                    Errors.NONE,
                    registerBroker(env.channelManager, null, 
                            new BrokerRegistrationRequestData.Feature()
                                    .setName(MetadataVersion.FEATURE_NAME)
                                    .setMinSupportedVersion(MetadataVersion.IBP_3_3_IV3.featureLevel())
                                    .setMaxSupportedVersion(MetadataVersion.IBP_3_4_IV0.featureLevel()))
            );
        }
    }

    final class ChannelEnv implements AutoCloseable {
        final Metrics metrics;
        final NodeToControllerChannelManager channelManager;

        ChannelEnv(ClusterInstance clusterInstance) {
            this.metrics = new Metrics();
            this.channelManager = brokerToControllerChannelManager(clusterInstance, metrics);
            this.channelManager.start();
        }

        @Override
        public void close() throws InterruptedException {
            channelManager.shutdown();
            metrics.close();
        }
    }

    ChannelEnv openChannel() {
        return new ChannelEnv(clusterInstance);
    }

    NodeToControllerChannelManager brokerToControllerChannelManager(ClusterInstance clusterInstance, Metrics metrics) {
        var controllerSocketServer = clusterInstance.controllers().values().stream()
                .map(ControllerServer::socketServer)
                .findFirst()
                .orElseThrow();

        return new NodeToControllerChannelManagerImpl(
                new TestControllerNodeProvider(clusterInstance),
                Time.SYSTEM,
                metrics,
                controllerSocketServer.config(),
                "heartbeat",
                "test-heartbeat-",
                10000L
        );
    }

    @SuppressWarnings("unchecked")
    <T extends AbstractRequest, R extends AbstractResponse> R sendAndReceive(
            NodeToControllerChannelManager channelManager,
            AbstractRequest.Builder<T> reqBuilder
    ) throws Exception {
        var responseFuture = new CompletableFuture<R>();
        channelManager.sendRequest(reqBuilder, new ControllerRequestCompletionHandler() {
            @Override
            public void onTimeout() {
                responseFuture.completeExceptionally(new TimeoutException());
            }

            @Override
            public void onComplete(ClientResponse response) {
                responseFuture.complete((R) response.responseBody());
            }
        });
        return responseFuture.get(30000, TimeUnit.MILLISECONDS);
    }

    Errors registerBroker(
            NodeToControllerChannelManager channelManager,
            Long zkEpoch,
            BrokerRegistrationRequestData.Feature featureLevelToSend
    ) throws Exception {
        var features = new BrokerRegistrationRequestData.FeatureCollection();

        if (featureLevelToSend != null) {
            features.add(featureLevelToSend);
        }

        Feature.PRODUCTION_FEATURES.stream()
                .filter(feature -> !feature.featureName().equals(MetadataVersion.FEATURE_NAME))
                .forEach(feature -> features.add(new BrokerRegistrationRequestData.Feature()
                        .setName(feature.featureName())
                        .setMinSupportedVersion(feature.minimumProduction())
                        .setMaxSupportedVersion(feature.latestTesting())));

        var listener = new BrokerRegistrationRequestData.Listener()
                .setName("EXTERNAL")
                .setHost("example.com")
                .setPort(8082)
                .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id);

        var req = new BrokerRegistrationRequestData()
                .setBrokerId(100)
                .setLogDirs(List.of(Uuid.randomUuid()))
                .setClusterId(clusterInstance.clusterId())
                .setIncarnationId(Uuid.randomUuid())
                .setIsMigratingZkBroker(zkEpoch != null)
                .setFeatures(features)
                .setListeners(new BrokerRegistrationRequestData.ListenerCollection(List.of(listener).iterator()));

        BrokerRegistrationResponse resp = this.sendAndReceive(
                channelManager,
                new BrokerRegistrationRequest.Builder(req)
        );
        return Errors.forCode(resp.data().errorCode());
    }

    record TestControllerNodeProvider(ClusterInstance clusterInstance)
            implements Supplier<ControllerInformation> {

        public Optional<Node> node() {
            return Optional.of(new Node(
                    clusterInstance.controllers().keySet().iterator().next(),
                    "127.0.0.1",
                    clusterInstance.controllerBoundPorts().get(0)
            ));
        }
        
        public ListenerName listenerName() {
            return clusterInstance.controllerListenerName();
        }

        public SecurityProtocol securityProtocol() {
            return SecurityProtocol.PLAINTEXT;
        }

        public String saslMechanism() {
            return "";
        }

        @Override
        public ControllerInformation get() {
            return new ControllerInformation(
                    node(),
                    listenerName(),
                    securityProtocol(),
                    saslMechanism()
            );
        }
    }
}