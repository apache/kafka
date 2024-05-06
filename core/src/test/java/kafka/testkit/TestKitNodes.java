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

package kafka.testkit;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestKitNodes {
    public static final int CONTROLLER_ID_OFFSET = 3000;
    public static final int BROKER_ID_OFFSET = 0;

    public static class Builder {
        private boolean combined;
        private Uuid clusterId;
        private int numControllerNodes;
        private int numBrokerNodes;
        private int numDisksPerBroker = 1;
        private Map<Integer, Map<String, String>> perServerProperties = Collections.emptyMap();
        private BootstrapMetadata bootstrapMetadata = BootstrapMetadata.
            fromVersion(MetadataVersion.latestTesting(), "testkit");

        public Builder setClusterId(Uuid clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder setBootstrapMetadataVersion(MetadataVersion metadataVersion) {
            this.bootstrapMetadata = BootstrapMetadata.fromVersion(metadataVersion, "testkit");
            return this;
        }

        public Builder setBootstrapMetadata(BootstrapMetadata bootstrapMetadata) {
            this.bootstrapMetadata = bootstrapMetadata;
            return this;
        }

        public Builder setCombined(boolean combined) {
            this.combined = combined;
            return this;
        }

        public Builder setNumControllerNodes(int numControllerNodes) {
            this.numControllerNodes = numControllerNodes;
            return this;
        }

        public Builder setNumBrokerNodes(int numBrokerNodes) {
            this.numBrokerNodes = numBrokerNodes;
            return this;
        }

        public Builder setNumDisksPerBroker(int numDisksPerBroker) {
            this.numDisksPerBroker = numDisksPerBroker;
            return this;
        }

        public Builder setPerServerProperties(Map<Integer, Map<String, String>> perServerProperties) {
            this.perServerProperties = Collections.unmodifiableMap(
                perServerProperties.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableMap(new HashMap<>(e.getValue())))));
            return this;
        }

        public TestKitNodes build() {
            if (numControllerNodes < 0) {
                throw new IllegalArgumentException("Invalid negative value for numControllerNodes");
            }
            if (numBrokerNodes < 0) {
                throw new IllegalArgumentException("Invalid negative value for numBrokerNodes");
            }
            if (numDisksPerBroker <= 0) {
                throw new IllegalArgumentException("Invalid value for numDisksPerBroker");
            }

            String baseDirectory = TestUtils.tempDirectory().getAbsolutePath();
            if (clusterId == null) {
                clusterId = Uuid.randomUuid();
            }

            int controllerId = combined ? BROKER_ID_OFFSET : BROKER_ID_OFFSET + CONTROLLER_ID_OFFSET;
            List<Integer> controllerNodeIds = IntStream.range(controllerId, controllerId + numControllerNodes)
                .boxed()
                .collect(Collectors.toList());
            List<Integer> brokerNodeIds = IntStream.range(BROKER_ID_OFFSET, BROKER_ID_OFFSET + numBrokerNodes)
                .boxed()
                .collect(Collectors.toList());

            String unknownIds = perServerProperties.keySet().stream()
                    .filter(id -> !controllerNodeIds.contains(id))
                    .filter(id -> !brokerNodeIds.contains(id))
                    .map(Object::toString)
                    .collect(Collectors.joining(", "));
            if (!unknownIds.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format("Unknown server id %s in perServerProperties, the existent server ids are %s",
                                unknownIds,
                                Stream.concat(brokerNodeIds.stream(), controllerNodeIds.stream())
                                        .map(Object::toString)
                                        .collect(Collectors.joining(", "))));
            }

            TreeMap<Integer, ControllerNode> controllerNodes = new TreeMap<>();
            for (int id : controllerNodeIds) {
                ControllerNode controllerNode = ControllerNode.builder()
                    .setId(id)
                    .setBaseDirectory(baseDirectory)
                    .setClusterId(clusterId)
                    .setCombined(brokerNodeIds.contains(id))
                    .setPropertyOverrides(perServerProperties.getOrDefault(id, Collections.emptyMap()))
                    .build();
                controllerNodes.put(id, controllerNode);
            }

            TreeMap<Integer, BrokerNode> brokerNodes = new TreeMap<>();
            for (int id : brokerNodeIds) {
                BrokerNode brokerNode = BrokerNode.builder()
                    .setId(id)
                    .setNumLogDirectories(numDisksPerBroker)
                    .setBaseDirectory(baseDirectory)
                    .setClusterId(clusterId)
                    .setCombined(controllerNodeIds.contains(id))
                    .setPropertyOverrides(perServerProperties.getOrDefault(id, Collections.emptyMap()))
                    .build();
                brokerNodes.put(id, brokerNode);
            }

            return new TestKitNodes(baseDirectory,
                clusterId,
                bootstrapMetadata,
                controllerNodes,
                brokerNodes);
        }
    }

    private final String baseDirectory;
    private final Uuid clusterId;
    private final BootstrapMetadata bootstrapMetadata;
    private final SortedMap<Integer, ControllerNode> controllerNodes;
    private final SortedMap<Integer, BrokerNode> brokerNodes;

    private TestKitNodes(
        String baseDirectory,
        Uuid clusterId,
        BootstrapMetadata bootstrapMetadata,
        SortedMap<Integer, ControllerNode> controllerNodes,
        SortedMap<Integer, BrokerNode> brokerNodes
    ) {
        this.baseDirectory = Objects.requireNonNull(baseDirectory);
        this.clusterId = Objects.requireNonNull(clusterId);
        this.bootstrapMetadata = Objects.requireNonNull(bootstrapMetadata);
        this.controllerNodes = Collections.unmodifiableSortedMap(new TreeMap<>(Objects.requireNonNull(controllerNodes)));
        this.brokerNodes = Collections.unmodifiableSortedMap(new TreeMap<>(Objects.requireNonNull(brokerNodes)));
    }

    public boolean isCombined(int node) {
        return controllerNodes.containsKey(node) && brokerNodes.containsKey(node);
    }

    public String baseDirectory() {
        return baseDirectory;
    }

    public Uuid clusterId() {
        return clusterId;
    }

    public SortedMap<Integer, ControllerNode> controllerNodes() {
        return controllerNodes;
    }

    public BootstrapMetadata bootstrapMetadata() {
        return bootstrapMetadata;
    }

    public SortedMap<Integer, BrokerNode> brokerNodes() {
        return brokerNodes;
    }

    public ListenerName interBrokerListenerName() {
        return new ListenerName("EXTERNAL");
    }

    public ListenerName externalListenerName() {
        return new ListenerName("EXTERNAL");
    }

    public ListenerName controllerListenerName() {
        return new ListenerName("CONTROLLER");
    }
}
