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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TestKitNodes {
    public static class Builder {
        private boolean combined = false;
        private Uuid clusterId = null;
        private MetadataVersion bootstrapMetadataVersion = null;
        private final NavigableMap<Integer, ControllerNode.Builder> controllerNodeBuilders = new TreeMap<>();
        private final NavigableMap<Integer, BrokerNode.Builder> brokerNodeBuilders = new TreeMap<>();

        public Builder setClusterId(Uuid clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder setBootstrapMetadataVersion(MetadataVersion metadataVersion) {
            this.bootstrapMetadataVersion = metadataVersion;
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
                controllerNodeBuilders.put(nextId, new ControllerNode.Builder().
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
                brokerNodeBuilders.put(nextId, new BrokerNode.Builder().
                    setId(nextId));
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
            String baseDirectory = TestUtils.tempDirectory("kafka_" + clusterId).getAbsolutePath();
            try {
                NavigableMap<Integer, ControllerNode> controllerNodes = new TreeMap<>();
                for (ControllerNode.Builder controllerNodeBuilder : controllerNodeBuilders.values()) {
                    ControllerNode controllerNode = controllerNodeBuilder.build(baseDirectory);
                    if (controllerNodes.put(controllerNode.id(), controllerNode) != null) {
                        throw new RuntimeException("More than one controller claimed ID " + controllerNode.id());
                    }
                }
                NavigableMap<Integer, BrokerNode> brokerNodes = new TreeMap<>();
                for (BrokerNode.Builder brokerNodeBuilder : brokerNodeBuilders.values()) {
                    BrokerNode brokerNode = brokerNodeBuilder.build(baseDirectory);
                    if (brokerNodes.put(brokerNode.id(), brokerNode) != null) {
                        throw new RuntimeException("More than one broker claimed ID " + brokerNode.id());
                    }
                }
                return new TestKitNodes(baseDirectory,
                    clusterId,
                    bootstrapMetadataVersion,
                    controllerNodes,
                    brokerNodes);
            } catch (Exception e) {
                try {
                    Utils.delete(new File(baseDirectory));
                } catch (IOException x) {
                    throw new RuntimeException(x);
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
    private final MetadataVersion bootstrapMetadataVersion;
    private final NavigableMap<Integer, ControllerNode> controllerNodes;
    private final NavigableMap<Integer, BrokerNode> brokerNodes;

    public boolean isCombined(int node) {
        return controllerNodes.containsKey(node) && brokerNodes.containsKey(node);
    }

    private TestKitNodes(
        String baseDirectory,
        Uuid clusterId,
        MetadataVersion bootstrapMetadataVersion,
        NavigableMap<Integer, ControllerNode> controllerNodes,
        NavigableMap<Integer, BrokerNode> brokerNodes
    ) {
        this.baseDirectory = baseDirectory;
        this.clusterId = clusterId;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.controllerNodes = controllerNodes;
        this.brokerNodes = brokerNodes;
    }

    public String baseDirectory() {
        return baseDirectory;
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

    public ListenerName controllerListenerName() {
        return new ListenerName("CONTROLLER");
    }

    static List<String> absolutize(String base, Collection<String> directories) {
        List<String> newDirectories = new ArrayList<>();
        for (String directory : directories) {
            newDirectories.add(absolutize(base, directory));
        }
        return newDirectories;
    }

    static String absolutize(String base, String directory) {
        if (Paths.get(directory).isAbsolute()) {
            return directory;
        }
        return Paths.get(base, directory).toAbsolutePath().toString();
    }
}
