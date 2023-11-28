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
import java.util.Optional;

public class ControllerNode implements TestKitNode {
    public static class Builder {
        private int id = -1;
        private String baseDirectory = null;
        private String metadataDirectory = null;
        private Uuid clusterId = null;

        public int id() {
            return id;
        }

        public Builder setId(int id) {
            this.id = id;
            return this;
        }

        public Builder setMetadataDirectory(String metadataDirectory) {
            this.metadataDirectory = metadataDirectory;
            return this;
        }

        public ControllerNode build(
            String baseDirectory,
            Uuid clusterId,
            boolean combined
        ) {
            if (id == -1) {
                throw new RuntimeException("You must set the node id.");
            }
            if (baseDirectory == null) {
                throw new RuntimeException("You must set the base directory.");
            }
            if (metadataDirectory == null) {
                if (combined) {
                    metadataDirectory = String.format("combined_%d", id);
                } else {
                    metadataDirectory = String.format("controller_%d", id);
                }
            }
            if (!Paths.get(metadataDirectory).isAbsolute()) {
                metadataDirectory = new File(baseDirectory, metadataDirectory).getAbsolutePath();
            }
            MetaPropertiesEnsemble.Copier copier =
                new MetaPropertiesEnsemble.Copier(MetaPropertiesEnsemble.EMPTY);
            copier.setMetaLogDir(Optional.of(metadataDirectory));
            copier.setLogDirProps(metadataDirectory, new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setClusterId(clusterId.toString()).
                setNodeId(id).
                setDirectoryId(copier.generateValidDirectoryId()).
                build());
            return new ControllerNode(copier.copy(), combined);
        }
    }

    private final MetaPropertiesEnsemble initialMetaPropertiesEnsemble;

    private final boolean combined;

    ControllerNode(
        MetaPropertiesEnsemble initialMetaPropertiesEnsemble,
        boolean combined
    ) {
        this.initialMetaPropertiesEnsemble = initialMetaPropertiesEnsemble;
        this.combined = combined;
    }

    @Override
    public MetaPropertiesEnsemble initialMetaPropertiesEnsemble() {
        return initialMetaPropertiesEnsemble;
    }

    @Override
    public boolean combined() {
        return combined;
    }
}
