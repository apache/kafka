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

package org.apache.kafka.common.test;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@SuppressWarnings("NPathComplexity")
public class TestKitNodes {

    public static final int CONTROLLER_ID_OFFSET = 3000;
    public static final int BROKER_ID_OFFSET = 0;
    public static final SecurityProtocol DEFAULT_BROKER_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;
    public static final String DEFAULT_BROKER_LISTENER_NAME = "EXTERNAL";
    public static final SecurityProtocol DEFAULT_CONTROLLER_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;
    public static final String DEFAULT_CONTROLLER_LISTENER_NAME = "CONTROLLER";

    public static class Builder {
        private boolean combined;
        private String clusterId;
        private Path baseDirectory;
        private int numControllerNodes;
        private int numBrokerNodes;
        private int numDisksPerBroker = 1;
        private Map<Integer, Map<String, String>> perServerProperties = Collections.emptyMap();
        private BootstrapMetadata bootstrapMetadata;

        public Builder() {
            this(BootstrapMetadata.fromVersion(MetadataVersion.latestTesting(), "testkit"));
        }

        public Builder(BootstrapMetadata bootstrapMetadata) {
            this.bootstrapMetadata = bootstrapMetadata;
        }
        // The broker and controller listener name and SecurityProtocol configurations must
        // be kept in sync with the default values in ClusterTest.
        private ListenerName brokerListenerName = ListenerName.normalised(DEFAULT_BROKER_LISTENER_NAME);
        private SecurityProtocol brokerSecurityProtocol = DEFAULT_BROKER_SECURITY_PROTOCOL;
        private ListenerName controllerListenerName = ListenerName.normalised(DEFAULT_CONTROLLER_LISTENER_NAME);
        private SecurityProtocol controllerSecurityProtocol = DEFAULT_CONTROLLER_SECURITY_PROTOCOL;

        public Builder setClusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder setBootstrapMetadataVersion(MetadataVersion metadataVersion) {
            this.bootstrapMetadata = bootstrapMetadata.copyWithFeatureRecord(
                    MetadataVersion.FEATURE_NAME, metadataVersion.featureLevel());
            return this;
        }

        public Builder setBootstrapMetadata(BootstrapMetadata bootstrapMetadata) {
            this.bootstrapMetadata = bootstrapMetadata;
            return this;
        }

        public Builder setFeature(String featureName, short level) {
            this.bootstrapMetadata = bootstrapMetadata.copyWithFeatureRecord(featureName, level);
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
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Map.copyOf(e.getValue()))));
            return this;
        }

        public Builder setBaseDirectory(Path baseDirectory) {
            this.baseDirectory = baseDirectory;
            return this;
        }

        public Builder setBrokerListenerName(ListenerName listenerName) {
            this.brokerListenerName = listenerName;
            return this;
        }

        public Builder setBrokerSecurityProtocol(SecurityProtocol securityProtocol) {
            this.brokerSecurityProtocol = securityProtocol;
            return this;
        }

        public Builder setControllerListenerName(ListenerName listenerName) {
            this.controllerListenerName = listenerName;
            return this;
        }

        public Builder setControllerSecurityProtocol(SecurityProtocol securityProtocol) {
            this.controllerSecurityProtocol = securityProtocol;
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
            // TODO: remove this assertion after https://issues.apache.org/jira/browse/KAFKA-16680 is finished
            if (brokerSecurityProtocol != SecurityProtocol.PLAINTEXT || controllerSecurityProtocol != SecurityProtocol.PLAINTEXT) {
                throw new IllegalArgumentException("Currently only support PLAINTEXT security protocol");
            }
            if (baseDirectory == null) {
                this.baseDirectory = TestUtils.tempDirectory().toPath();
            }
            if (clusterId == null) {
                clusterId = Uuid.randomUuid().toString();
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

            TreeMap<Integer, TestKitNode> controllerNodes = new TreeMap<>();
            for (int id : controllerNodeIds) {
                TestKitNode controllerNode = TestKitNodes.buildControllerNode(
                    id,
                    baseDirectory.toFile().getAbsolutePath(),
                    clusterId,
                    brokerNodeIds.contains(id),
                    perServerProperties.getOrDefault(id, Collections.emptyMap())
                );
                controllerNodes.put(id, controllerNode);
            }

            TreeMap<Integer, TestKitNode> brokerNodes = new TreeMap<>();
            for (int id : brokerNodeIds) {
                TestKitNode brokerNode = TestKitNodes.buildBrokerNode(
                    id,
                    baseDirectory.toFile().getAbsolutePath(),
                    clusterId,
                    controllerNodeIds.contains(id),
                    perServerProperties.getOrDefault(id, Collections.emptyMap()),
                    numDisksPerBroker
                );
                brokerNodes.put(id, brokerNode);
            }

            return new TestKitNodes(baseDirectory.toFile().getAbsolutePath(), clusterId, bootstrapMetadata, controllerNodes, brokerNodes,
                brokerListenerName, brokerSecurityProtocol, controllerListenerName, controllerSecurityProtocol);
        }
    }

    private final String baseDirectory;
    private final String clusterId;
    private final BootstrapMetadata bootstrapMetadata;
    private final SortedMap<Integer, TestKitNode> controllerNodes;
    private final SortedMap<Integer, TestKitNode> brokerNodes;
    private final ListenerName brokerListenerName;
    private final ListenerName controllerListenerName;
    private final SecurityProtocol brokerSecurityProtocol;
    private final SecurityProtocol controllerSecurityProtocol;

    private TestKitNodes(
        String baseDirectory,
        String clusterId,
        BootstrapMetadata bootstrapMetadata,
        SortedMap<Integer, TestKitNode> controllerNodes,
        SortedMap<Integer, TestKitNode> brokerNodes,
        ListenerName brokerListenerName,
        SecurityProtocol brokerSecurityProtocol,
        ListenerName controllerListenerName,
        SecurityProtocol controllerSecurityProtocol
    ) {
        this.baseDirectory = Objects.requireNonNull(baseDirectory);
        this.clusterId = Objects.requireNonNull(clusterId);
        this.bootstrapMetadata = Objects.requireNonNull(bootstrapMetadata);
        this.controllerNodes = Collections.unmodifiableSortedMap(new TreeMap<>(Objects.requireNonNull(controllerNodes)));
        this.brokerNodes = Collections.unmodifiableSortedMap(new TreeMap<>(Objects.requireNonNull(brokerNodes)));
        this.brokerListenerName = Objects.requireNonNull(brokerListenerName);
        this.controllerListenerName = Objects.requireNonNull(controllerListenerName);
        this.brokerSecurityProtocol = Objects.requireNonNull(brokerSecurityProtocol);
        this.controllerSecurityProtocol = Objects.requireNonNull(controllerSecurityProtocol);
    }

    public boolean isCombined(int node) {
        return controllerNodes.containsKey(node) && brokerNodes.containsKey(node);
    }

    public String baseDirectory() {
        return baseDirectory;
    }

    public String clusterId() {
        return clusterId;
    }

    public SortedMap<Integer, TestKitNode> controllerNodes() {
        return controllerNodes;
    }

    public BootstrapMetadata bootstrapMetadata() {
        return bootstrapMetadata;
    }

    public SortedMap<Integer, TestKitNode> brokerNodes() {
        return brokerNodes;
    }

    public ListenerName brokerListenerName() {
        return brokerListenerName;
    }

    public SecurityProtocol brokerListenerProtocol() {
        return brokerSecurityProtocol;
    }

    public ListenerName controllerListenerName() {
        return controllerListenerName;
    }

    public SecurityProtocol controllerListenerProtocol() {
        return controllerSecurityProtocol;
    }

    private static TestKitNode buildBrokerNode(int id,
                                              String baseDirectory,
                                              String clusterId,
                                              boolean combined,
                                              Map<String, String> propertyOverrides,
                                              int numDisksPerBroker) {
        List<String> logDataDirectories = IntStream
            .range(0, numDisksPerBroker)
            .mapToObj(i -> {
                if (combined) {
                    return String.format("combined_%d_%d", id, i);
                }
                return String.format("broker_%d_data%d", id, i);
            })
            .map(logDir -> {
                if (Paths.get(logDir).isAbsolute()) {
                    return logDir;
                }
                return new File(baseDirectory, logDir).getAbsolutePath();
            })
            .collect(Collectors.toList());
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(MetaPropertiesEnsemble.EMPTY);

        copier.setMetaLogDir(Optional.of(logDataDirectories.get(0)));
        for (String logDir : logDataDirectories) {
            copier.setLogDirProps(
                logDir,
                new MetaProperties.Builder()
                    .setVersion(MetaPropertiesVersion.V1)
                    .setClusterId(clusterId)
                    .setNodeId(id)
                    .setDirectoryId(copier.generateValidDirectoryId())
                    .build()
            );
        }

        return new TestKitNode() {
            private final MetaPropertiesEnsemble ensemble = copier.copy();

            @Override
            public MetaPropertiesEnsemble initialMetaPropertiesEnsemble() {
                return ensemble;
            }

            @Override
            public Map<String, String> propertyOverrides() {
                return Collections.unmodifiableMap(propertyOverrides);
            }
        };
    }

    private static TestKitNode buildControllerNode(int id,
                                                  String baseDirectory,
                                                  String clusterId,
                                                  boolean combined,
                                                  Map<String, String> propertyOverrides) {
        String metadataDirectory = new File(baseDirectory,
            combined ? String.format("combined_%d_0", id) : String.format("controller_%d", id)).getAbsolutePath();
        MetaPropertiesEnsemble.Copier copier = new MetaPropertiesEnsemble.Copier(MetaPropertiesEnsemble.EMPTY);

        copier.setMetaLogDir(Optional.of(metadataDirectory));
        copier.setLogDirProps(
            metadataDirectory,
            new MetaProperties.Builder()
                .setVersion(MetaPropertiesVersion.V1)
                .setClusterId(clusterId)
                .setNodeId(id)
                .setDirectoryId(copier.generateValidDirectoryId())
                .build()
        );

        return new TestKitNode() {
            private final MetaPropertiesEnsemble ensemble = copier.copy();

            @Override
            public MetaPropertiesEnsemble initialMetaPropertiesEnsemble() {
                return ensemble;
            }

            @Override
            public Map<String, String> propertyOverrides() {
                return Collections.unmodifiableMap(propertyOverrides);
            }
        };
    }
}
