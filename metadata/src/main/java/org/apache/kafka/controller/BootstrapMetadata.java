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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.metadata.util.SnapshotFileReader;
import org.apache.kafka.metadata.util.SnapshotFileWriter;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;


/**
 * A read-only class that holds the controller bootstrap metadata. A file named "bootstrap.snapshot" is used and the
 * format is the same as a KRaft snapshot.
 */
public class BootstrapMetadata {
    private static final Logger log = LoggerFactory.getLogger(BootstrapMetadata.class);

    public static final String BOOTSTRAP_FILE = "bootstrap.checkpoint";

    private final MetadataVersion metadataVersion;

    private final List<ApiMessageAndVersion> records;

    BootstrapMetadata(MetadataVersion metadataVersion, List<ApiMessageAndVersion> records) {
        this.metadataVersion = metadataVersion;
        this.records = Collections.unmodifiableList(records);
    }

    public MetadataVersion metadataVersion() {
        return this.metadataVersion;
    }

    public List<ApiMessageAndVersion> records() {
        return records;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BootstrapMetadata metadata = (BootstrapMetadata) o;
        return metadataVersion == metadata.metadataVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(metadataVersion);
    }

    @Override
    public String toString() {
        return "BootstrapMetadata{" +
            "metadataVersion=" + metadataVersion +
            '}';
    }

    /**
     * A raft client listener that simply collects all of the commits and snapshots into a mapping of
     * metadata record type to list of records.
     */
    private static class BootstrapListener implements RaftClient.Listener<ApiMessageAndVersion> {
        private final List<ApiMessageAndVersion> records = new ArrayList<>();

        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            try {
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    records.addAll(batch.records());
                }
            } finally {
                reader.close();
            }
        }

        @Override
        public void handleSnapshot(SnapshotReader<ApiMessageAndVersion> reader) {
            try {
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    for (ApiMessageAndVersion messageAndVersion : batch) {
                        records.add(messageAndVersion);
                    }
                }
            } finally {
                reader.close();
            }
        }
    }

    public static BootstrapMetadata create(MetadataVersion metadataVersion) {
        return create(metadataVersion, new ArrayList<>());
    }

    public static BootstrapMetadata create(MetadataVersion metadataVersion, List<ApiMessageAndVersion> records) {
        if (!metadataVersion.isKRaftSupported()) {
            throw new IllegalArgumentException("Cannot create BootstrapMetadata with a non-KRaft metadata version.");
        }
        records.add(new ApiMessageAndVersion(
            new FeatureLevelRecord()
                .setName(MetadataVersion.FEATURE_NAME)
                .setFeatureLevel(metadataVersion.featureLevel()),
            FeatureLevelRecord.LOWEST_SUPPORTED_VERSION));

        return new BootstrapMetadata(metadataVersion, records);
    }

    /**
     * Load a bootstrap snapshot into a read-only bootstrap metadata object and return it.
     *
     * @param bootstrapDir  The directory from which to read the snapshot file.
     * @param fallbackPreviewVersion    The metadata.version to boostrap if upgrading from KRaft preview
     * @return              The read-only bootstrap metadata
     * @throws Exception
     */
    public static BootstrapMetadata load(Path bootstrapDir, MetadataVersion fallbackPreviewVersion) throws Exception {
        final Path bootstrapPath = bootstrapDir.resolve(BOOTSTRAP_FILE);

        if (!Files.exists(bootstrapPath)) {
            log.debug("Missing bootstrap file, this appears to be a KRaft preview cluster. Setting metadata.version to {}.",
                fallbackPreviewVersion.featureLevel());
            return BootstrapMetadata.create(fallbackPreviewVersion);
        }

        BootstrapListener listener = new BootstrapListener();
        try (SnapshotFileReader reader = new SnapshotFileReader(bootstrapPath.toString(), listener)) {
            reader.startup();
            reader.caughtUpFuture().get();
        } catch (ExecutionException e) {
            throw new Exception("Failed to load snapshot", e.getCause());
        }

        Optional<FeatureLevelRecord> metadataVersionRecord = listener.records.stream()
            .flatMap(message -> {
                MetadataRecordType type = MetadataRecordType.fromId(message.message().apiKey());
                if (!type.equals(MetadataRecordType.FEATURE_LEVEL_RECORD)) {
                    return Stream.empty();
                }
                FeatureLevelRecord record = (FeatureLevelRecord) message.message();
                if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
                    return Stream.of(record);
                } else {
                    return Stream.empty();
                }
            })
            .findFirst();

        if (metadataVersionRecord.isPresent()) {
            return new BootstrapMetadata(MetadataVersion.fromFeatureLevel(metadataVersionRecord.get().featureLevel()), listener.records);
        } else {
            throw new RuntimeException("Expected a metadata.version to exist in the snapshot " + bootstrapPath + ", but none was found");
        }
    }

    /**
     * Write a set of bootstrap metadata to the bootstrap snapshot in a given directory
     *
     * @param metadata      The metadata to persist
     * @param bootstrapDir  The directory in which to create the bootstrap snapshot
     * @throws IOException
     */
    public static void write(BootstrapMetadata metadata, Path bootstrapDir) throws IOException {
        final Path bootstrapPath = bootstrapDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE);
        if (Files.exists(bootstrapPath)) {
            throw new IOException("Cannot write metadata bootstrap file " + bootstrapPath +
                ". File already already exists.");
        }
        try (SnapshotFileWriter bootstrapWriter = SnapshotFileWriter.open(bootstrapPath)) {
            bootstrapWriter.append(metadata.records());
        }
    }
}
