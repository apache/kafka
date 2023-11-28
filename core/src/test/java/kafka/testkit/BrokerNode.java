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
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BrokerNode implements TestKitNode {
    public static class Builder {
        private int id = -1;
        private String baseDirectory = null;
        private Uuid clusterId = null;
        private Uuid incarnationId = null;
        private List<String> logDataDirectories = null;
        private String metadataDirectory = null;
        private Map<String, String> propertyOverrides = new HashMap<>();

        public int id() {
            return id;
        }

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setIncarnationId(Uuid incarnationId) {
            this.incarnationId = incarnationId;
            return this;
        }

        public Builder setMetadataDirectory(String metadataDirectory) {
            this.metadataDirectory = metadataDirectory;
            return this;
        }

        public Builder setLogDirectories(List<String> logDataDirectories) {
            this.logDataDirectories = logDataDirectories;
            return this;
        }

        public BrokerNode build(
            String baseDirectory,
            Uuid clusterId,
            boolean combined
        ) {
            if (id == -1) {
                throw new RuntimeException("You must set the node id.");
            }
            if (incarnationId == null) {
                incarnationId = Uuid.randomUuid();
            }
            if (logDataDirectories == null) {
                if (combined) {
                    logDataDirectories = Collections.
                        singletonList(String.format("combined_%d", id));
                } else {
                    logDataDirectories = Collections.
                        singletonList(String.format("broker_%d_data0", id));
                }
            }
            List<String> absoluteLogDataDirectories = new ArrayList<>();
            for (String logDir : logDataDirectories) {
                if (Paths.get(logDir).isAbsolute()) {
                    absoluteLogDataDirectories.add(logDir);
                } else {
                    absoluteLogDataDirectories.add(new File(baseDirectory, logDir).getAbsolutePath());
                }
            }
            this.logDataDirectories = absoluteLogDataDirectories;
            if (metadataDirectory == null) {
                metadataDirectory = logDataDirectories.get(0);
            } else if (!Paths.get(metadataDirectory).isAbsolute()) {
                metadataDirectory = new File(baseDirectory, metadataDirectory).getAbsolutePath();
            }
            MetaPropertiesEnsemble.Copier copier =
                    new MetaPropertiesEnsemble.Copier(MetaPropertiesEnsemble.EMPTY);
            copier.setMetaLogDir(Optional.of(metadataDirectory));
            for (String logDir : logDataDirectories) {
                copier.setLogDirProps(logDir, new MetaProperties.Builder().
                    setVersion(MetaPropertiesVersion.V1).
                    setClusterId(clusterId.toString()).
                    setNodeId(id).
                    setDirectoryId(copier.generateValidDirectoryId()).
                    build());
            }
            copier.setMetaLogDir(Optional.of(metadataDirectory));
            return new BrokerNode(incarnationId,
                copier.copy(),
                combined,
                propertyOverrides);
        }
    }

    private final Uuid incarnationId;
    private final MetaPropertiesEnsemble initialMetaPropertiesEnsemble;
    private final boolean combined;
    private final Map<String, String> propertyOverrides;

    BrokerNode(
        Uuid incarnationId,
        MetaPropertiesEnsemble initialMetaPropertiesEnsemble,
        boolean combined,
        Map<String, String> propertyOverrides
    ) {
        this.incarnationId = incarnationId;
        this.initialMetaPropertiesEnsemble = initialMetaPropertiesEnsemble;
        this.combined = combined;
        this.propertyOverrides = new HashMap<>(propertyOverrides);
    }

    public Uuid incarnationId() {
        return incarnationId;
    }

    @Override
    public MetaPropertiesEnsemble initialMetaPropertiesEnsemble() {
        return initialMetaPropertiesEnsemble;
    }

    @Override
    public boolean combined() {
        return combined;
    }

    public List<String> logDataDirectories() {
        return new ArrayList<>(initialMetaPropertiesEnsemble.logDirProps().keySet());
    }

    public Map<String, String> propertyOverrides() {
        return propertyOverrides;
    }
}
