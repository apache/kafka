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

import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface TestKitNode {
    default int id() {
        return initialMetaPropertiesEnsemble().nodeId().getAsInt();
    }

    default String metadataDirectory() {
        return initialMetaPropertiesEnsemble().metadataLogDir().get();
    }

    default Set<String> logDataDirectories() {
        return initialMetaPropertiesEnsemble().logDirProps().keySet();
    }

    MetaPropertiesEnsemble initialMetaPropertiesEnsemble();

    Map<String, String> propertyOverrides();


    class Builder {
        private int id = -1;
        private String baseDirectory;
        private String clusterId;
        private int numLogDirectories = 1;
        private Map<String, String> propertyOverrides = Collections.emptyMap();
        private boolean combined;

        protected Builder() {}

        public int id() {
            return id;
        }

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setNumLogDirectories(int numLogDirectories) {
            this.numLogDirectories = numLogDirectories;
            return this;
        }

        public Builder setClusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder setBaseDirectory(String baseDirectory) {
            this.baseDirectory = baseDirectory;
            return this;
        }

        public Builder setCombined(boolean combined) {
            this.combined = combined;
            return this;
        }

        public Builder setPropertyOverrides(Map<String, String> propertyOverrides) {
            this.propertyOverrides = Collections.unmodifiableMap(new HashMap<>(propertyOverrides));
            return this;
        }

        public TestKitNode buildBrokerNode() {
            Objects.requireNonNull(baseDirectory);
            Objects.requireNonNull(clusterId);
            if (id == -1) {
                throw new IllegalArgumentException("You must set the node id.");
            }
            if (numLogDirectories < 1) {
                throw new IllegalArgumentException("The value of numLogDirectories should be at least 1.");
            }
            List<String> logDataDirectories = IntStream
                .range(0, numLogDirectories)
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
            MetaPropertiesEnsemble.Copier copier =
                new MetaPropertiesEnsemble.Copier(MetaPropertiesEnsemble.EMPTY);
            copier.setMetaLogDir(Optional.of(logDataDirectories.get(0)));
            for (String logDir : logDataDirectories) {
                copier.setLogDirProps(logDir, new MetaProperties.Builder().
                    setVersion(MetaPropertiesVersion.V1).
                    setClusterId(clusterId).
                    setNodeId(id).
                    setDirectoryId(copier.generateValidDirectoryId()).
                    build());
            }
            return new TestKitNode() {
                private final MetaPropertiesEnsemble ensemble = copier.copy();

                @Override
                public MetaPropertiesEnsemble initialMetaPropertiesEnsemble() {
                    return ensemble;
                }

                @Override
                public Map<String, String> propertyOverrides() {
                    return propertyOverrides;
                }
            };
        }

        public TestKitNode buildControllerNode() {
            if (id == -1) {
                throw new IllegalArgumentException("You must set the node id.");
            }
            if (baseDirectory == null) {
                throw new IllegalArgumentException("You must set the base directory.");
            }
            String metadataDirectory = new File(baseDirectory,
                combined ? String.format("combined_%d_0", id) : String.format("controller_%d", id)).getAbsolutePath();
            MetaPropertiesEnsemble.Copier copier =
                new MetaPropertiesEnsemble.Copier(MetaPropertiesEnsemble.EMPTY);
            copier.setMetaLogDir(Optional.of(metadataDirectory));
            copier.setLogDirProps(metadataDirectory, new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setClusterId(clusterId).
                setNodeId(id).
                setDirectoryId(copier.generateValidDirectoryId()).
                build());
            return new TestKitNode() {
                private final MetaPropertiesEnsemble ensemble = copier.copy();

                @Override
                public MetaPropertiesEnsemble initialMetaPropertiesEnsemble() {
                    return ensemble;
                }

                @Override
                public Map<String, String> propertyOverrides() {
                    return propertyOverrides;
                }
            };
        }
    }
}
