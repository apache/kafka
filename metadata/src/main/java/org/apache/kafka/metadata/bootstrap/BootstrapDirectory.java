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

import org.apache.kafka.metadata.util.BatchFileReader;
import org.apache.kafka.metadata.util.BatchFileReader.BatchAndType;
import org.apache.kafka.metadata.util.BatchFileWriter;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;


/**
 * A read-only class that holds the controller bootstrap metadata. A file named "bootstrap.checkpoint" is used and the
 * format is the same as a KRaft snapshot.
 */
public class BootstrapDirectory {
    public static final String BINARY_BOOTSTRAP_FILENAME = "bootstrap.checkpoint";

    private final String directoryPath;
    private final Optional<String> ibp;

    /**
     * Create a new BootstrapDirectory object.
     *
     * @param directoryPath     The path to the directory with the bootstrap file.
     * @param ibp               The configured value of inter.broker.protocol, or the empty string
     *                          if it is not configured.
     */
    public BootstrapDirectory(
        String directoryPath,
        Optional<String> ibp
    ) {
        this.directoryPath = Objects.requireNonNull(directoryPath);
        this.ibp = Objects.requireNonNull(ibp);
    }

    public BootstrapMetadata read() throws Exception {
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
        if (ibp.isEmpty()) {
            return BootstrapMetadata.fromVersion(MetadataVersion.latestProduction(), "the default bootstrap");
        }
        MetadataVersion version = MetadataVersion.fromVersionString(ibp.get());
        if (version.isLessThan(MINIMUM_BOOTSTRAP_VERSION)) {
            return BootstrapMetadata.fromVersion(MINIMUM_BOOTSTRAP_VERSION,
                "the minimum version bootstrap with metadata.version " + MINIMUM_BOOTSTRAP_VERSION);
        }
        return BootstrapMetadata.fromVersion(version,
            "the configured bootstrap with metadata.version " + version);
    }

    BootstrapMetadata readFromBinaryFile(String binaryPath) throws Exception {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        try (BatchFileReader reader = new BatchFileReader.Builder().
                setPath(binaryPath).build()) {
            while (reader.hasNext()) {
                BatchAndType batchAndType = reader.next();
                if (!batchAndType.isControl()) {
                    records.addAll(batchAndType.batch().records());
                }
            }
        }
        return BootstrapMetadata.fromRecords(Collections.unmodifiableList(records),
                "the binary bootstrap metadata file: " + binaryPath);
    }

    public void writeBinaryFile(BootstrapMetadata bootstrapMetadata) throws IOException {
        if (!Files.isDirectory(Paths.get(directoryPath))) {
            throw new RuntimeException("No such directory as " + directoryPath);
        }
        Path tempPath = Paths.get(directoryPath, BINARY_BOOTSTRAP_FILENAME + ".tmp");
        Files.deleteIfExists(tempPath);
        try {
            try (BatchFileWriter writer = BatchFileWriter.open(tempPath)) {
                for (ApiMessageAndVersion message : bootstrapMetadata.records()) {
                    writer.append(message);
                }
            }

            Files.move(
                tempPath,
                Paths.get(directoryPath, BINARY_BOOTSTRAP_FILENAME),
                ATOMIC_MOVE, REPLACE_EXISTING
            );
        } finally {
            Files.deleteIfExists(tempPath);
        }
    }
}
