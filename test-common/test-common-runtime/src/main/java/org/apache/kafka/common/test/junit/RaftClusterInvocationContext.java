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
package org.apache.kafka.common.test.junit;

import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.KafkaBroker;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.storage.FormatterException;
import org.apache.kafka.server.common.Feature;
import org.apache.kafka.server.common.FeatureVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.FaultHandlerException;

import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * </ul>
 */
public class RaftClusterInvocationContext implements TestTemplateInvocationContext {

    private final String baseDisplayName;
    private final ClusterConfig clusterConfig;
    private final boolean isCombined;

    /**
     * Wait for condition to be met for at most 15 seconds and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    static void waitForCondition(final java.util.function.Supplier<Boolean> testCondition,
                                        final String conditionDetails) throws InterruptedException {
        var maxWaitMs = 15_000L;
        long endTime = System.currentTimeMillis() + maxWaitMs;

        while (System.currentTimeMillis() < endTime) {
            try {
                if (testCondition.get()) {
                    return;
                }
            } catch (Exception e) {
                if (System.currentTimeMillis() >= endTime) {
                    throw new AssertionError(String.format("Assertion failed with an exception after %s ms", maxWaitMs), e);
                }
            }

            if (System.currentTimeMillis() < endTime) {
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
        throw new AssertionError("Condition not met: " + conditionDetails);
    }

    public RaftClusterInvocationContext(String baseDisplayName, ClusterConfig clusterConfig, boolean isCombined) {
        this.baseDisplayName = baseDisplayName;
        this.clusterConfig = clusterConfig;
        this.isCombined = isCombined;
    }

    @Override
    public String getDisplayName(int invocationIndex) {
        return String.format("%s [%d] Type=Raft-%s, %s", baseDisplayName, invocationIndex, isCombined ? "Combined" : "Isolated", String.join(",", clusterConfig.displayTags()));
    }

    @Override
    public List<Extension> getAdditionalExtensions() {
        RaftClusterInstance clusterInstance = new RaftClusterInstance(clusterConfig, isCombined);
        return List.of(
                (BeforeEachCallback) context -> {
                    clusterInstance.format();
                    if (clusterConfig.isAutoStart()) {
                        clusterInstance.start();
                    }
                },
                (AfterTestExecutionCallback) context -> clusterInstance.stop(),
                new ClusterInstanceParameterResolver(clusterInstance)
        );
    }

    private static class RaftClusterInstance implements ClusterInstance {

        private final ClusterConfig clusterConfig;
        final AtomicBoolean started = new AtomicBoolean(false);
        final AtomicBoolean stopped = new AtomicBoolean(false);
        final AtomicBoolean formated = new AtomicBoolean(false);
        private KafkaClusterTestKit clusterTestKit;
        private final boolean isCombined;
        private final ListenerName listenerName;

        RaftClusterInstance(ClusterConfig clusterConfig, boolean isCombined) {
            this.clusterConfig = clusterConfig;
            this.isCombined = isCombined;
            this.listenerName = clusterConfig.brokerListenerName();
        }

        @Override
        public String bootstrapServers() {
            return clusterTestKit.bootstrapServers();
        }

        @Override
        public String bootstrapControllers() {
            return clusterTestKit.bootstrapControllers();
        }

        @Override
        public ListenerName clientListener() {
            return listenerName;
        }

        @Override
        public ListenerName controllerListenerName() {
            return new ListenerName(
                controllers()
                    .values()
                    .iterator()
                    .next()
                    .config()
                    .controllerListenerNames()
                    .get(0)
            );
        }

        @Override
        public String clusterId() {
            return Stream.concat(controllers().values().stream().map(ControllerServer::clusterId),
                brokers().values().stream().map(KafkaBroker::clusterId)).findFirst()
                .orElseThrow(() -> new RuntimeException("No controllers or brokers!"));
        }

        @Override
        public Type type() {
            return isCombined ? Type.CO_KRAFT : Type.KRAFT;
        }

        @Override
        public ClusterConfig config() {
            return clusterConfig;
        }

        @Override
        public Set<Integer> controllerIds() {
            return controllers().keySet();
        }

        public KafkaClusterTestKit getUnderlying() {
            return clusterTestKit;
        }

        @Override
        public void start() {
            try {
                format();
                if (started.compareAndSet(false, true)) {
                    clusterTestKit.startup();
                    waitForCondition(
                            () -> this.clusterTestKit.brokers().values().stream().allMatch(
                                    brokers -> brokers.brokerState() == BrokerState.RUNNING
                            ), "Broker never made it to RUNNING state.");
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to start Raft server", e);
            }
        }

        @Override
        public boolean started() {
            return started.get();
        }

        @Override
        public void stop() {
            if (stopped.compareAndSet(false, true)) {
                Utils.closeQuietly(clusterTestKit, "cluster");
            }
        }

        @Override
        public boolean stopped() {
            return stopped.get();
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
        public Optional<FaultHandlerException> firstFatalException() {
            return Optional.ofNullable(clusterTestKit.fatalFaultHandler().firstException());
        }

        @Override
        public Optional<FaultHandlerException> firstNonFatalException() {
            return Optional.ofNullable(clusterTestKit.nonFatalFaultHandler().firstException());
        }

        @Override
        public void waitForReadyBrokers() throws InterruptedException {
            try {
                clusterTestKit.waitForReadyBrokers();
            } catch (ExecutionException e) {
                throw new AssertionError("Failed while waiting for brokers to become ready", e);
            }
        }

        @Override
        public Map<Integer, KafkaBroker> brokers() {
            return clusterTestKit.brokers().entrySet()
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public Map<Integer, ControllerServer> controllers() {
            return Collections.unmodifiableMap(clusterTestKit.controllers());
        }

        public void format() throws Exception {
            if (formated.compareAndSet(false, true)) {
                Map<String, Feature> nameToSupportedFeature = new TreeMap<>();
                Feature.PRODUCTION_FEATURES.forEach(feature -> nameToSupportedFeature.put(feature.featureName(), feature));
                Map<String, Short> newFeatureLevels = new TreeMap<>();

                // Verify that all specified features are known to us.
                for (Map.Entry<Feature, Short> entry : clusterConfig.features().entrySet()) {
                    String featureName = entry.getKey().featureName();
                    short level = entry.getValue();
                    if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                        if (!nameToSupportedFeature.containsKey(featureName)) {
                            throw new FormatterException("Unsupported feature: " + featureName +
                                ". Supported features are: " + String.join(", ", nameToSupportedFeature.keySet()));
                        }
                    }
                    newFeatureLevels.put(featureName, level);
                }
                newFeatureLevels.put(MetadataVersion.FEATURE_NAME, clusterConfig.metadataVersion().featureLevel());

                // Add default values for features that were not specified.
                Feature.PRODUCTION_FEATURES.forEach(supportedFeature -> {
                    if (!newFeatureLevels.containsKey(supportedFeature.featureName())) {
                        newFeatureLevels.put(supportedFeature.featureName(),
                            supportedFeature.defaultLevel(clusterConfig.metadataVersion()));
                    }
                });

                // Verify that the specified features support the given levels. This requires the full
                // features map since there may be cross-feature dependencies.
                for (Map.Entry<String, Short> entry : newFeatureLevels.entrySet()) {
                    String featureName = entry.getKey();
                    if (!featureName.equals(MetadataVersion.FEATURE_NAME)) {
                        short level = entry.getValue();
                        Feature supportedFeature = nameToSupportedFeature.get(featureName);
                        FeatureVersion featureVersion =
                            supportedFeature.fromFeatureLevel(level, true);
                        Feature.validateVersion(featureVersion, newFeatureLevels);
                    }
                }

                TestKitNodes nodes = new TestKitNodes.Builder()
                        .setBootstrapMetadata(BootstrapMetadata.fromVersions(clusterConfig.metadataVersion(), newFeatureLevels, "testkit"))
                        .setCombined(isCombined)
                        .setNumBrokerNodes(clusterConfig.numBrokers())
                        .setNumDisksPerBroker(clusterConfig.numDisksPerBroker())
                        .setPerServerProperties(clusterConfig.perServerOverrideProperties())
                        .setNumControllerNodes(clusterConfig.numControllers())
                        .setBrokerListenerName(listenerName)
                        .setBrokerSecurityProtocol(clusterConfig.brokerSecurityProtocol())
                        .setControllerListenerName(clusterConfig.controllerListenerName())
                        .setControllerSecurityProtocol(clusterConfig.controllerSecurityProtocol())
                        .build();
                KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
                // Copy properties into the TestKit builder
                clusterConfig.serverProperties().forEach(builder::setConfigProp);
                builder.setStandalone(clusterConfig.standalone());
                this.clusterTestKit = builder.build();
                this.clusterTestKit.format();
            }
        }

        private BrokerServer findBrokerOrThrow(int brokerId) {
            return Optional.ofNullable(clusterTestKit.brokers().get(brokerId))
                    .orElseThrow(() -> new IllegalArgumentException("Unknown brokerId " + brokerId));
        }

    }
}
