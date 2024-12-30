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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.metadata.FakeKafkaConfigSchema;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metalog.LocalLogManagerTestEnv;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.test.TestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class QuorumControllerTestEnv implements AutoCloseable {
    private final List<QuorumController> controllers;
    private final LocalLogManagerTestEnv logEnv;
    private final Map<Integer, MockFaultHandler> fatalFaultHandlers = new HashMap<>();
    private final Map<Integer, MockFaultHandler> nonFatalFaultHandlers = new HashMap<>();

    public static class Builder {
        private final LocalLogManagerTestEnv logEnv;
        private Consumer<QuorumController.Builder> controllerBuilderInitializer = __ -> { };
        private OptionalLong sessionTimeoutMillis = OptionalLong.empty();
        private OptionalLong leaderImbalanceCheckIntervalNs = OptionalLong.empty();
        private BootstrapMetadata bootstrapMetadata = BootstrapMetadata.
                fromVersion(MetadataVersion.latestTesting(), "test-provided version");

        public Builder(LocalLogManagerTestEnv logEnv) {
            this.logEnv = logEnv;
        }

        public Builder setControllerBuilderInitializer(Consumer<QuorumController.Builder> controllerBuilderInitializer) {
            this.controllerBuilderInitializer = controllerBuilderInitializer;
            return this;
        }

        public Builder setSessionTimeoutMillis(OptionalLong sessionTimeoutMillis) {
            this.sessionTimeoutMillis = sessionTimeoutMillis;
            return this;
        }

        public Builder setLeaderImbalanceCheckIntervalNs(OptionalLong leaderImbalanceCheckIntervalNs) {
            this.leaderImbalanceCheckIntervalNs = leaderImbalanceCheckIntervalNs;
            return this;
        }

        public Builder setBootstrapMetadata(BootstrapMetadata bootstrapMetadata) {
            this.bootstrapMetadata = bootstrapMetadata;
            return this;
        }

        public QuorumControllerTestEnv build() throws Exception {
            return new QuorumControllerTestEnv(
                logEnv,
                controllerBuilderInitializer,
                sessionTimeoutMillis,
                leaderImbalanceCheckIntervalNs,
                bootstrapMetadata.metadataVersion().isElrSupported(),
                bootstrapMetadata);
        }
    }

    private QuorumControllerTestEnv(
        LocalLogManagerTestEnv logEnv,
        Consumer<QuorumController.Builder> controllerBuilderInitializer,
        OptionalLong sessionTimeoutMillis,
        OptionalLong leaderImbalanceCheckIntervalNs,
        boolean eligibleLeaderReplicasEnabled,
        BootstrapMetadata bootstrapMetadata
    ) throws Exception {
        this.logEnv = logEnv;
        int numControllers = logEnv.logManagers().size();
        this.controllers = new ArrayList<>(numControllers);
        try {
            List<Integer> nodeIds = IntStream.range(0, numControllers).boxed().collect(Collectors.toList());
            for (int nodeId = 0; nodeId < numControllers; nodeId++) {
                QuorumController.Builder builder = new QuorumController.Builder(nodeId, logEnv.clusterId());
                builder.setRaftClient(logEnv.logManagers().get(nodeId));
                builder.setBootstrapMetadata(bootstrapMetadata);
                builder.setLeaderImbalanceCheckIntervalNs(leaderImbalanceCheckIntervalNs);
                builder.setQuorumFeatures(new QuorumFeatures(nodeId, QuorumFeatures.defaultFeatureMap(true), nodeIds));
                sessionTimeoutMillis.ifPresent(timeout ->
                    builder.setSessionTimeoutNs(NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS))
                );
                MockFaultHandler fatalFaultHandler = new MockFaultHandler("fatalFaultHandler");
                builder.setFatalFaultHandler(fatalFaultHandler);
                fatalFaultHandlers.put(nodeId, fatalFaultHandler);
                MockFaultHandler nonFatalFaultHandler = new MockFaultHandler("nonFatalFaultHandler");
                builder.setNonFatalFaultHandler(nonFatalFaultHandler);
                builder.setConfigSchema(FakeKafkaConfigSchema.INSTANCE);
                nonFatalFaultHandlers.put(nodeId, fatalFaultHandler);
                controllerBuilderInitializer.accept(builder);
                QuorumController controller = builder.build();
                if (eligibleLeaderReplicasEnabled) {
                    controller.featureControl().replay(new FeatureLevelRecord()
                        .setName(EligibleLeaderReplicasVersion.FEATURE_NAME)
                        .setFeatureLevel(EligibleLeaderReplicasVersion.ELRV_1.featureLevel())
                    );
                }
                this.controllers.add(controller);
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    QuorumController activeController() throws InterruptedException {
        return activeController(false);
    }

    QuorumController activeController(boolean waitForActivation) throws InterruptedException {
        AtomicReference<QuorumController> value = new AtomicReference<>(null);
        TestUtils.retryOnExceptionWithTimeout(20000, 3, () -> {
            LeaderAndEpoch leader = logEnv.leaderAndEpoch();
            for (QuorumController controller : controllers) {
                if (OptionalInt.of(controller.nodeId()).equals(leader.leaderId()) &&
                    controller.curClaimEpoch() == leader.epoch()) {
                    value.set(controller);
                    break;
                }
            }

            if (value.get() == null) {
                throw new RuntimeException(String.format("Expected to see %s as leader", leader));
            }
        });

        if (waitForActivation) {
            try {
                // ControllerActivation happens after curClaimEpoch is set, so we need to put something on
                // the end of the queue and wait for it to complete before returning the active controller.
                value.get()
                    .appendReadEvent("wait for activation", OptionalLong.empty(), () -> null)
                    .get(20000, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                throw new RuntimeException("Failed while waiting for controller activation", t);
            }
        }

        return value.get();
    }

    public List<QuorumController> controllers() {
        return controllers;
    }

    public MockFaultHandler fatalFaultHandler(Integer nodeId) {
        return fatalFaultHandlers.get(nodeId);
    }

    public void ignoreFatalFaults() {
        for (MockFaultHandler faultHandler : fatalFaultHandlers.values()) {
            faultHandler.setIgnore(true);
        }
    }

    @Override
    public void close() throws InterruptedException {
        for (QuorumController controller : controllers) {
            controller.beginShutdown();
        }
        for (QuorumController controller : controllers) {
            controller.close();
        }
        for (MockFaultHandler faultHandler : fatalFaultHandlers.values()) {
            faultHandler.maybeRethrowFirstException();
        }
        for (MockFaultHandler faultHandler : nonFatalFaultHandlers.values()) {
            faultHandler.maybeRethrowFirstException();
        }
    }
}
