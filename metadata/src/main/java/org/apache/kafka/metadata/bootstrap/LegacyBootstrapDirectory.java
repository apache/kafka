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

package org.apache.kafka.metadata.bootstrap;

import org.apache.kafka.server.common.MetadataVersion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;


/**
 * Reads bootstrap metadata from the legacy {@code bootstrap.checkpoint} file.
 */
public class LegacyBootstrapDirectory implements BootstrapDirectory {
    public static final String BINARY_BOOTSTRAP_FILENAME = "bootstrap.checkpoint";

    private final String directoryPath;

    public LegacyBootstrapDirectory(String directoryPath) {
        this.directoryPath = Objects.requireNonNull(directoryPath);
    }

    @Override
    public BootstrapMetadata read() throws IOException {
        Path path = Paths.get(directoryPath);
        if (!Files.isDirectory(path)) {
            if (Files.exists(path)) {
                throw new RuntimeException("Path " + directoryPath + " exists, but is not " +
                        "a directory.");
            } else {
                throw new RuntimeException("No such directory as " + directoryPath);
            }
        }
        Path binaryBootstrapPath = Paths.get(directoryPath, BINARY_BOOTSTRAP_FILENAME);
        if (!Files.exists(binaryBootstrapPath)) {
            return readFromConfiguration();
        } else {
            return readFromBinaryFile(binaryBootstrapPath.toString());
        }
    }

    BootstrapMetadata readFromConfiguration() {
        return BootstrapMetadata.fromVersion(MetadataVersion.latestProduction(), "the default bootstrap");
    }
}
