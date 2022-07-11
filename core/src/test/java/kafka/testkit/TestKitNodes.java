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

import kafka.server.MetaProperties;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.server.common.MetadataVersion;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TestKitNodes {
    public static class Builder {
        private boolean coResident = false;
        private Uuid clusterId = null;
        private MetadataVersion bootstrapMetadataVersion = null;
        private final NavigableMap<Integer, ControllerNode> controllerNodes = new TreeMap<>();
        private final NavigableMap<Integer, BrokerNode> brokerNodes = new TreeMap<>();

        public Builder setClusterId(Uuid clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder setBootstrapMetadataVersion(MetadataVersion metadataVersion) {
            this.bootstrapMetadataVersion = metadataVersion;
            return this;
        }

        public Builder setCoResident(boolean coResident) {
            this.coResident = coResident;
            return this;
        }

        public Builder addNodes(TestKitNode[] nodes) {
            for (TestKitNode node : nodes) {
                addNode(node);
            }
            return this;
        }

        public Builder addNode(TestKitNode node) {
            if (node instanceof ControllerNode) {
                ControllerNode controllerNode = (ControllerNode) node;
                controllerNodes.put(node.id(), controllerNode);
            } else if (node instanceof BrokerNode) {
                BrokerNode brokerNode = (BrokerNode) node;
                brokerNodes.put(node.id(), brokerNode);
            } else {
                throw new RuntimeException("Can't handle TestKitNode subclass " +
                        node.getClass().getSimpleName());
            }
            return this;
        }

        public Builder setNumControllerNodes(int numControllerNodes) {
            if (numControllerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numControllerNodes");
            }

            while (controllerNodes.size() > numControllerNodes) {
                controllerNodes.pollFirstEntry();
            }
            while (controllerNodes.size() < numControllerNodes) {
                int nextId = startControllerId();
                if (!controllerNodes.isEmpty()) {
                    nextId = controllerNodes.lastKey() + 1;
                }
                controllerNodes.put(nextId, new ControllerNode.Builder().
                    setId(nextId).build());
            }
            return this;
        }

        public Builder setNumBrokerNodes(int numBrokerNodes) {
            if (numBrokerNodes < 0) {
                throw new RuntimeException("Invalid negative value for numBrokerNodes");
            }
            while (brokerNodes.size() > numBrokerNodes) {
                brokerNodes.pollFirstEntry();
            }
            while (brokerNodes.size() < numBrokerNodes) {
                int nextId = startBrokerId();
                if (!brokerNodes.isEmpty()) {
                    nextId = brokerNodes.lastKey() + 1;
                }
                brokerNodes.put(nextId, new BrokerNode.Builder().
                    setId(nextId).build());
            }
            return this;
        }

        public TestKitNodes build() {
            if (clusterId == null) {
                clusterId = Uuid.randomUuid();
            }
            if (bootstrapMetadataVersion == null) {
                bootstrapMetadataVersion = MetadataVersion.latest();
            }
            return new TestKitNodes(clusterId, bootstrapMetadataVersion, controllerNodes, brokerNodes);
        }

        private int startBrokerId() {
            return 0;
        }

        private int startControllerId() {
            if (coResident) {
                return startBrokerId();
            }
            return startBrokerId() + 3000;
        }
    }

    private final Uuid clusterId;
    private final MetadataVersion bootstrapMetadataVersion;
    private final NavigableMap<Integer, ControllerNode> controllerNodes;
    private final NavigableMap<Integer, BrokerNode> brokerNodes;

    public boolean isCoResidentNode(int node) {
        return controllerNodes.containsKey(node) && brokerNodes.containsKey(node);
    }

    private TestKitNodes(Uuid clusterId,
                         MetadataVersion bootstrapMetadataVersion,
                         NavigableMap<Integer, ControllerNode> controllerNodes,
                         NavigableMap<Integer, BrokerNode> brokerNodes) {
        this.clusterId = clusterId;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.controllerNodes = controllerNodes;
        this.brokerNodes = brokerNodes;
    }

    public Uuid clusterId() {
        return clusterId;
    }

    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadataVersion;
    }

    public Map<Integer, ControllerNode> controllerNodes() {
        return controllerNodes;
    }

    public NavigableMap<Integer, BrokerNode> brokerNodes() {
        return brokerNodes;
    }

    public MetaProperties controllerProperties(int id) {
        return MetaProperties.apply(clusterId.toString(), id);
    }

    public MetaProperties brokerProperties(int id) {
        return MetaProperties.apply(clusterId.toString(), id);
    }

    public ListenerName interBrokerListenerName() {
        return new ListenerName("EXTERNAL");
    }

    public ListenerName externalListenerName() {
        return new ListenerName("EXTERNAL");
    }

    public TestKitNodes copyWithAbsolutePaths(String baseDirectory) {
        NavigableMap<Integer, ControllerNode> newControllerNodes = new TreeMap<>();
        NavigableMap<Integer, BrokerNode> newBrokerNodes = new TreeMap<>();
        for (Entry<Integer, ControllerNode> entry : controllerNodes.entrySet()) {
            ControllerNode node = entry.getValue();
            newControllerNodes.put(entry.getKey(), new ControllerNode(node.id(),
                absolutize(baseDirectory, node.metadataDirectory())));
        }
        for (Entry<Integer, BrokerNode> entry : brokerNodes.entrySet()) {
            BrokerNode node = entry.getValue();
            newBrokerNodes.put(entry.getKey(), new BrokerNode(node.id(),
                node.incarnationId(), absolutize(baseDirectory, node.metadataDirectory()),
                absolutize(baseDirectory, node.logDataDirectories()), node.propertyOverrides()));
        }
        return new TestKitNodes(clusterId, bootstrapMetadataVersion, newControllerNodes, newBrokerNodes);
    }

    private static List<String> absolutize(String base, Collection<String> directories) {
        List<String> newDirectories = new ArrayList<>();
        for (String directory : directories) {
            newDirectories.add(absolutize(base, directory));
        }
        return newDirectories;
    }

    private static String absolutize(String base, String directory) {
        if (Paths.get(directory).isAbsolute()) {
            return directory;
        }
        return Paths.get(base, directory).toAbsolutePath().toString();
    }
}
