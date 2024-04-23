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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TestKitNodes {
    public static class Builder {
        private boolean combined;
        private Uuid clusterId;
        private int numControllerNodes;
        private int numBrokerNodes;
        private int numDisksPerBroker = 1;
        private Map<Integer, Map<String, String>> perBrokerProperties = Collections.emptyMap();
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

        public Builder setPerBrokerProperties(Map<Integer, Map<String, String>> perBrokerProperties) {
            this.perBrokerProperties = Collections.unmodifiableMap(
                perBrokerProperties.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableMap(new HashMap<>(e.getValue())))));
            return this;
        }

        public TestKitNodes build() {
            if (numControllerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numControllerNodes");
            }
            if (numBrokerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numBrokerNodes");
            }
            if (numDisksPerBroker <= 0) {
                throw new RuntimeException("Invalid value for numDisksPerBroker");
            }

            String baseDirectory = TestUtils.tempDirectory().getAbsolutePath();
            try {
                if (clusterId == null) {
                    clusterId = Uuid.randomUuid();
                }
                TreeMap<Integer, ControllerNode> controllerNodes = new TreeMap<>();
                for (int id = startControllerId(); id < startControllerId() + numControllerNodes; id++) {
                    ControllerNode node = ControllerNode.builder()
                        .setId(id)
                        .setBaseDirectory(baseDirectory)
                        .setClusterId(clusterId)
                        .setCombined(combined)
                        .build();
                    controllerNodes.put(node.id(), node);
                }
                TreeMap<Integer, BrokerNode> brokerNodes = new TreeMap<>();
                for (int id = startBrokerId(); id < startBrokerId() + numBrokerNodes; id++) {
                    BrokerNode node = BrokerNode.builder()
                        .setId(id)
                        .setNumLogDirectories(numDisksPerBroker)
                        .setBaseDirectory(baseDirectory)
                        .setClusterId(clusterId)
                        .setCombined(combined)
                        .setPropertyOverrides(perBrokerProperties.getOrDefault(id, Collections.emptyMap()))
                        .build();
                    brokerNodes.put(node.id(), node);
                }
                return new TestKitNodes(baseDirectory,
                    clusterId,
                    bootstrapMetadata,
                    controllerNodes,
                    brokerNodes);
            } catch (Exception e) {
                try {
                    Files.delete(Paths.get(baseDirectory));
                } catch (Exception x) {
                    throw new RuntimeException("Failed to delete base directory " + baseDirectory, x);
                }
                throw e;
            }
        }

        private int startBrokerId() {
            return 0;
        }

        private int startControllerId() {
            if (combined) {
                return startBrokerId();
            }
            return startBrokerId() + 3000;
        }
    }

    private final String baseDirectory;
    private final Uuid clusterId;
    private final BootstrapMetadata bootstrapMetadata;
    private final NavigableMap<Integer, ControllerNode> controllerNodes;
    private final NavigableMap<Integer, BrokerNode> brokerNodes;

    private TestKitNodes(
        String baseDirectory,
        Uuid clusterId,
        BootstrapMetadata bootstrapMetadata,
        NavigableMap<Integer, ControllerNode> controllerNodes,
        NavigableMap<Integer, BrokerNode> brokerNodes
    ) {
        this.baseDirectory = Objects.requireNonNull(baseDirectory);
        this.clusterId = Objects.requireNonNull(clusterId);
        this.bootstrapMetadata = Objects.requireNonNull(bootstrapMetadata);
        this.controllerNodes = new TreeMap<>(Objects.requireNonNull(controllerNodes));
        this.brokerNodes = new TreeMap<>(Objects.requireNonNull(brokerNodes));
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

    public Map<Integer, ControllerNode> controllerNodes() {
        return controllerNodes;
    }

    public BootstrapMetadata bootstrapMetadata() {
        return bootstrapMetadata;
    }

    public NavigableMap<Integer, BrokerNode> brokerNodes() {
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
