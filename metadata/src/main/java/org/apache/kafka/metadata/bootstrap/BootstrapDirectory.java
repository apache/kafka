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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.apache.kafka.server.common.MetadataVersion.MINIMUM_KRAFT_VERSION;


/**
 * A read-only class that holds the controller bootstrap metadata. A file named "bootstrap.snapshot" is used and the
 * format is the same as a KRaft snapshot.
 */
public class BootstrapDirectory {
    final static String INTER_BROKER_PROTOCOL_CONFIG_KEY = "inter.broker.protocol.version";
    final static String BINARY_BOOTSTRAP = "bootstrap.checkpoint";

    public static String ibpStringFromConfigMap(Map<String, Object> staticConfig) {
        Object value = staticConfig.get(INTER_BROKER_PROTOCOL_CONFIG_KEY);
        return value == null ? "" : value.toString();
    }

    private final String directoryPath;
    private final String ibp;

    /**
     * Create a new BootstrapDirectory object.
     *
     * @param directoryPath     The path to the directory with the bootstrap file.
     * @param ibp               The configured value of inter.broker.protocol, or the empty string
     *                          if it is not configured.
     */
    public BootstrapDirectory(
        String directoryPath,
        String ibp
    ) {
        Objects.requireNonNull(directoryPath);
        Objects.requireNonNull(ibp);
        this.directoryPath = directoryPath;
        this.ibp = ibp;
    }

    public BootstrapMetadata read() throws Exception {
        if (!Files.isDirectory(Paths.get(directoryPath))) {
            throw new RuntimeException("No such directory as " + directoryPath);
        }
        Path binaryBootstrapPath = Paths.get(directoryPath, BINARY_BOOTSTRAP);
        if (!Files.exists(binaryBootstrapPath)) {
            return readFromConfiguration();
        } else {
            return readFromBinaryFile(binaryBootstrapPath.toString());
        }
    }

    BootstrapMetadata readFromConfiguration() {
        if (ibp.isEmpty()) {
            return BootstrapMetadata.fromVersion(MetadataVersion.latest(),
                    "the default bootstrap file which sets the latest metadata.version, " +
                    "since no bootstrap file was found, and " + INTER_BROKER_PROTOCOL_CONFIG_KEY +
                    " was not configured.");
        }
        MetadataVersion version = MetadataVersion.fromVersionString(ibp);
        if (version.isLessThan(MINIMUM_KRAFT_VERSION)) {
            return BootstrapMetadata.fromVersion(MINIMUM_KRAFT_VERSION,
                    "a default bootstrap file setting the minimum supported KRaft metadata " +
                    "version, since no bootstrap file was found, and " +
                    INTER_BROKER_PROTOCOL_CONFIG_KEY + " was " + version + ", which is not " +
                    "currently supported by KRaft.");
        }
        return BootstrapMetadata.fromVersion(version,
                "a default bootstrap file setting the metadata.version to " + version + ", as " +
                "specified by " + INTER_BROKER_PROTOCOL_CONFIG_KEY + ".");
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

    public void writeBinaryFile(BootstrapMetadata bootstrapMetadata) throws Exception {
        if (!Files.isDirectory(Paths.get(directoryPath))) {
            throw new RuntimeException("No such directory as " + directoryPath);
        }
        Path tempPath = Paths.get(directoryPath, BINARY_BOOTSTRAP + ".tmp");
        Files.deleteIfExists(tempPath);
        try (BatchFileWriter writer = BatchFileWriter.open(tempPath)) {
            for (ApiMessageAndVersion message : bootstrapMetadata.records()) {
                writer.append(message);
            }
        }
        Files.move(tempPath, Paths.get(directoryPath, BINARY_BOOTSTRAP),
                ATOMIC_MOVE, REPLACE_EXISTING);
    }
}
