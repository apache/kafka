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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TestKitNodes {
    public static class Builder {
        private boolean combined = false;
        private Uuid clusterId = null;
        private BootstrapMetadata bootstrapMetadata = BootstrapMetadata.
            fromVersion(MetadataVersion.latestTesting(), "testkit");
        private final NavigableMap<Integer, ControllerNode.Builder> controllerNodeBuilders = new TreeMap<>();
        private final NavigableMap<Integer, BrokerNode.Builder> brokerNodeBuilders = new TreeMap<>();

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
            if (numControllerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numControllerNodes");
            }

            while (controllerNodeBuilders.size() > numControllerNodes) {
                controllerNodeBuilders.pollFirstEntry();
            }
            while (controllerNodeBuilders.size() < numControllerNodes) {
                int nextId = startControllerId();
                if (!controllerNodeBuilders.isEmpty()) {
                    nextId = controllerNodeBuilders.lastKey() + 1;
                }
                controllerNodeBuilders.put(nextId,
                    new ControllerNode.Builder().
                        setId(nextId));
            }
            return this;
        }

        public Builder setNumBrokerNodes(int numBrokerNodes) {
            if (numBrokerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numBrokerNodes");
            }
            while (brokerNodeBuilders.size() > numBrokerNodes) {
                brokerNodeBuilders.pollFirstEntry();
            }
            while (brokerNodeBuilders.size() < numBrokerNodes) {
                int nextId = startBrokerId();
                if (!brokerNodeBuilders.isEmpty()) {
                    nextId = brokerNodeBuilders.lastKey() + 1;
                }
                brokerNodeBuilders.put(nextId,
                    new BrokerNode.Builder().
                        setId(nextId));
            }
            return this;
        }

        public TestKitNodes build() {
            String baseDirectory = TestUtils.tempDirectory().getAbsolutePath();
            try {
                if (clusterId == null) {
                    clusterId = Uuid.randomUuid();
                }
                TreeMap<Integer, ControllerNode> controllerNodes = new TreeMap<>();
                for (ControllerNode.Builder builder : controllerNodeBuilders.values()) {
                    ControllerNode node = builder.
                        build(baseDirectory, clusterId, brokerNodeBuilders.containsKey(builder.id()));
                    if (controllerNodes.put(node.id(), node) != null) {
                        throw new RuntimeException("Duplicate builder for controller " + node.id());
                    }
                }
                TreeMap<Integer, BrokerNode> brokerNodes = new TreeMap<>();
                for (BrokerNode.Builder builder : brokerNodeBuilders.values()) {
                    BrokerNode node = builder.
                        build(baseDirectory, clusterId, controllerNodeBuilders.containsKey(builder.id()));
                    if (brokerNodes.put(node.id(), node) != null) {
                        throw new RuntimeException("Duplicate builder for broker " + node.id());
                    }
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
        this.baseDirectory = baseDirectory;
        this.clusterId = clusterId;
        this.bootstrapMetadata = bootstrapMetadata;
        this.controllerNodes = controllerNodes;
        this.brokerNodes = brokerNodes;
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

    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadata.metadataVersion();
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
