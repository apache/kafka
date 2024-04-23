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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BrokerNode implements TestKitNode {
    public static class Builder {
        private int id = -1;
        private String baseDirectory = null;
        private Uuid clusterId = null;
        private Uuid incarnationId = null;
        private int numLogDirectories = 1;
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

        public Builder setNumLogDirectories(int numLogDirectories) {
            this.numLogDirectories = numLogDirectories;
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
