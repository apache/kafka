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

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.util.BatchFileReader;
import org.apache.kafka.metadata.util.BatchFileReader.BatchAndType;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The bootstrap metadata. On startup, if the metadata log is empty, we will populate the log with
 * these records. Alternately, if log is not empty, but the metadata version is not set, we will
 * use the version specified here.
 */
public class BootstrapMetadata {
    public static final String BINARY_BOOTSTRAP_FILENAME = "bootstrap.checkpoint";

    private final List<ApiMessageAndVersion> records;
    private final short metadataVersionLevel;
    private final String source;

    /**
     * Reads bootstrap metadata from the given directory. Checks the legacy bootstrap.checkpoint
     * first and falls back to defaults if it does not exist.
     */
    public static BootstrapMetadata fromDirectory(Path directory) {
        if (!Files.isDirectory(directory)) {
            if (Files.exists(directory)) {
                throw new IllegalStateException("Path " + directory + " exists, but is not " +
                        "a directory.");
            } else {
                throw new IllegalStateException("No such directory as " + directory);
            }
        }
        Path binaryBootstrapPath = directory.resolve(BINARY_BOOTSTRAP_FILENAME);
        if (Files.exists(binaryBootstrapPath)) {
            return fromCheckpointFile(binaryBootstrapPath);
        }
        return fromVersion(MetadataVersion.latestProduction(), "the default bootstrap");
    }

    /**
     * Reads bootstrap metadata from the given checkpoint file.
     * Throws if the file does not exist.
     */
    public static BootstrapMetadata fromCheckpointFile(Path file) {
        if (!Files.exists(file)) {
            String path = file.toString();
            throw new UncheckedIOException(path, new FileNotFoundException(path));
        }
        return readFromBinaryFile(file);
    }

    private static BootstrapMetadata readFromBinaryFile(Path binaryPath) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        try (BatchFileReader reader = new BatchFileReader.Builder().
                setPath(binaryPath.toString()).build()) {
            while (reader.hasNext()) {
                BatchAndType batchAndType = reader.next();
                if (!batchAndType.isControl()) {
                    records.addAll(batchAndType.batch().records());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read bootstrap metadata from " + binaryPath, e);
        }
        return fromRecords(Collections.unmodifiableList(records),
                "the binary bootstrap metadata file: " + binaryPath);
    }

    public static BootstrapMetadata fromVersions(
        MetadataVersion metadataVersion,
        Map<String, Short> featureVersions,
        String source
    ) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        records.add(new ApiMessageAndVersion(new FeatureLevelRecord().
            setName(MetadataVersion.FEATURE_NAME).
            setFeatureLevel(metadataVersion.featureLevel()), (short) 0));
        List<String> featureNames = new ArrayList<>(featureVersions.size());
        featureVersions.keySet().forEach(n -> {
            // metadata.version is handled in a special way, and kraft.version generates no
            // FeatureLevelRecord.
            if (!(n.equals(MetadataVersion.FEATURE_NAME) ||
                    n.equals(KRaftVersion.FEATURE_NAME))) {
                featureNames.add(n);
            }
        });
        featureNames.sort(String::compareTo);
        for (String featureName : featureNames) {
            short level = featureVersions.get(featureName);
            if (level > 0) {
                records.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                    setName(featureName).
                    setFeatureLevel(level), (short) 0));
            }
        }
        return new BootstrapMetadata(records, metadataVersion.featureLevel(), source);
    }

    public static BootstrapMetadata fromVersion(MetadataVersion metadataVersion, String source) {
        List<ApiMessageAndVersion> records = List.of(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()), (short) 0));
        return new BootstrapMetadata(records, metadataVersion.featureLevel(), source);
    }

    public static BootstrapMetadata fromRecords(List<ApiMessageAndVersion> records, String source) {
        Optional<Short> metadataVersionLevel = Optional.empty();
        for (ApiMessageAndVersion record : records) {
            Optional<Short> level = recordToMetadataVersionLevel(record.message());
            if (level.isPresent()) {
                metadataVersionLevel = level;
            }
        }
        if (metadataVersionLevel.isEmpty()) {
            throw new RuntimeException("No FeatureLevelRecord for " + MetadataVersion.FEATURE_NAME +
                    " was found in the bootstrap metadata from " + source);
        }
        return new BootstrapMetadata(records, metadataVersionLevel.get(), source);
    }

    private static Optional<Short> recordToMetadataVersionLevel(ApiMessage record) {
        if (record instanceof FeatureLevelRecord featureLevel) {
            if (featureLevel.name().equals(MetadataVersion.FEATURE_NAME)) {
                return Optional.of(featureLevel.featureLevel());
            }
        }
        return Optional.empty();
    }

    BootstrapMetadata(
        List<ApiMessageAndVersion> records,
        short metadataVersionLevel,
        String source
    ) {
        this.records = Objects.requireNonNull(records);
        this.metadataVersionLevel = metadataVersionLevel;
        Objects.requireNonNull(source);
        this.source = source;
    }

    public List<ApiMessageAndVersion> records() {
        return records;
    }

    public MetadataVersion metadataVersion() {
        return MetadataVersion.fromFeatureLevel(metadataVersionLevel);
    }

    public String source() {
        return source;
    }

    public short featureLevel(String featureName) {
        short result = 0;
        for (ApiMessageAndVersion record : records) {
            if (record.message() instanceof FeatureLevelRecord message) {
                if (message.name().equals(featureName)) {
                    result = message.featureLevel();
                }
            }
        }
        return result;
    }

    public BootstrapMetadata copyWithFeatureRecord(String featureName, short level) {
        List<ApiMessageAndVersion> newRecords = new ArrayList<>();
        int i = 0;
        while (i < records.size()) {
            if (records.get(i).message() instanceof FeatureLevelRecord record) {
                if (record.name().equals(featureName)) {
                    FeatureLevelRecord newRecord = record.duplicate();
                    newRecord.setFeatureLevel(level);
                    newRecords.add(new ApiMessageAndVersion(newRecord, (short) 0));
                    break;
                } else {
                    newRecords.add(records.get(i));
                }
            }
            i++;
        }
        if (i == records.size()) {
            FeatureLevelRecord newRecord = new FeatureLevelRecord().
                setName(featureName).
                setFeatureLevel(level);
            newRecords.add(new ApiMessageAndVersion(newRecord, (short) 0));
        }
        return BootstrapMetadata.fromRecords(newRecords, source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(records, metadataVersionLevel, source);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !o.getClass().equals(this.getClass())) return false;
        BootstrapMetadata other = (BootstrapMetadata) o;
        return Objects.equals(records, other.records) &&
            metadataVersionLevel == other.metadataVersionLevel &&
            source.equals(other.source);
    }

    @Override
    public String toString() {
        return "BootstrapMetadata(records=" + records +
            ", metadataVersionLevel=" + metadataVersionLevel +
            ", source=" + source +
            ")";
    }
}
