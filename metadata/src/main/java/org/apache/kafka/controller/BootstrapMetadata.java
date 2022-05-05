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
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.controller.util.SnapshotFileReader;
import org.apache.kafka.controller.util.SnapshotFileWriter;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


/**
 * A read-only class that holds the controller bootstrap metadata. A file named "bootstrap.snapshot" is used and the
 * format is the same as a KRaft snapshot.
 */
public class BootstrapMetadata {
    private static final Logger log = LoggerFactory.getLogger(BootstrapMetadata.class);

    public static final String BOOTSTRAP_FILE = "bootstrap.snapshot";

    private final MetadataVersion metadataVersion;

    BootstrapMetadata(MetadataVersion metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    public MetadataVersion metadataVersion() {
        return this.metadataVersion;
    }

    /**
     * A raft client listener that simply collects all of the commits and snapshots into a mapping of
     * metadata record type to list of records.
     */
    private static class BootstrapListener implements RaftClient.Listener<ApiMessageAndVersion> {
        private final Map<MetadataRecordType, List<ApiMessage>> messages = new LinkedHashMap<>();

        @Override
        public void handleCommit(BatchReader<ApiMessageAndVersion> reader) {
            try {
                while (reader.hasNext()) {
                    Batch<ApiMessageAndVersion> batch = reader.next();
                    for (ApiMessageAndVersion messageAndVersion : batch.records()) {
                        handleMessage(messageAndVersion.message());
                    }
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
                        handleMessage(messageAndVersion.message());
                    }
                }
            } finally {
                reader.close();
            }
        }

        void handleMessage(ApiMessage message) {
            try {
                MetadataRecordType type = MetadataRecordType.fromId(message.apiKey());
                messages.computeIfAbsent(type, __ -> new ArrayList<>()).add(message);
            } catch (Exception e) {
                log.error("Error processing record of type " + message.apiKey(), e);
            }
        }
    }

    public static BootstrapMetadata create(MetadataVersion metadataVersion) {
        return new BootstrapMetadata(metadataVersion);
    }

    /**
     * Load a bootstrap snapshot into a read-only bootstrap metadata object and return it.
     *
     * @param bootstrapDir  The directory from which to read the snapshot file.
     * @return              The read-only bootstrap metadata
     * @throws Exception
     */
    public static BootstrapMetadata load(Path bootstrapDir) throws Exception {
        final Path bootstrapPath = bootstrapDir.resolve(BOOTSTRAP_FILE);

        if (!Files.exists(bootstrapPath)) {
            log.debug("Missing bootstrap file, this appears to be a KRaft preview cluster.");
            return new BootstrapMetadata(MetadataVersion.IBP_3_0_IV0);
        }

        BootstrapListener listener = new BootstrapListener();
        SnapshotFileReader reader = new SnapshotFileReader(bootstrapPath.toString(), listener);
        reader.startup();
        reader.caughtUpFuture().get();

        List<ApiMessage> featureRecords = listener.messages.getOrDefault(MetadataRecordType.FEATURE_LEVEL_RECORD, Collections.emptyList());
        Optional<FeatureLevelRecord> metadataVersionRecord = featureRecords.stream()
                .map(apiMessage -> (FeatureLevelRecord) apiMessage)
                .filter(featureLevelRecord -> featureLevelRecord.name().equals(MetadataVersion.FEATURE_NAME))
                .findFirst();
        if (metadataVersionRecord.isPresent()) {
            return new BootstrapMetadata(MetadataVersion.fromFeatureLevel(metadataVersionRecord.get().featureLevel()));
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
        SnapshotFileWriter bootstrapWriter = SnapshotFileWriter.open(bootstrapPath);
        bootstrapWriter.append(
            new ApiMessageAndVersion(
                new FeatureLevelRecord()
                    .setName(MetadataVersion.FEATURE_NAME)
                    .setFeatureLevel(metadata.metadataVersion.featureLevel()),
                FeatureLevelRecord.LOWEST_SUPPORTED_VERSION));
        bootstrapWriter.close();
    }
}
