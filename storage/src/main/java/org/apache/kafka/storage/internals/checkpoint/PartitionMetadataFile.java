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

package org.apache.kafka.storage.internals.checkpoint;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class PartitionMetadataFile {
    private static final String PARTITION_METADATA_FILE_NAME = "partition.metadata";
    static final int CURRENT_VERSION = 0;

    public static File newFile(File dir) {
        return new File(dir, PARTITION_METADATA_FILE_NAME);
    }

    private final File file;
    private final LogDirFailureChannel logDirFailureChannel;

    private final Object lock = new Object();
    private volatile Optional<Uuid> dirtyTopicIdOpt = Optional.empty();

    public PartitionMetadataFile(
        final File file,
        final LogDirFailureChannel logDirFailureChannel
    ) {
        this.file = file;
        this.logDirFailureChannel = logDirFailureChannel;
    }

    /**
     * Records the topic ID that will be flushed to disk.
     */
    public void record(Uuid topicId) {
        // Topic IDs should not differ, but we defensively check here to fail earlier in the case that the IDs somehow differ.
        dirtyTopicIdOpt.ifPresent(dirtyTopicId -> {
            if (!dirtyTopicId.equals(topicId)) {
                throw new InconsistentTopicIdException("Tried to record topic ID " + topicId + " to file " +
                        "but had already recorded " + dirtyTopicId);
            }
        });
        dirtyTopicIdOpt = Optional.of(topicId);
    }

    public void maybeFlush() {
        // We check dirtyTopicId first to avoid having to take the lock unnecessarily in the frequently called log append path
        if (dirtyTopicIdOpt.isPresent()) {
            // We synchronize on the actual write to disk
            synchronized (lock) {
                dirtyTopicIdOpt.ifPresent(topicId -> {
                    try {
                        try (FileOutputStream fileOutputStream = new FileOutputStream(tempPath().toFile());
                             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
                            writer.write(new PartitionMetadata(CURRENT_VERSION, topicId).encode());
                            writer.flush();
                            fileOutputStream.getFD().sync();
                        }

                        Utils.atomicMoveWithFallback(tempPath(), path());
                    } catch (IOException e) {
                        String msg = "Error while writing partition metadata file " + file.getAbsolutePath();
                        logDirFailureChannel.maybeAddOfflineLogDir(logDir(), msg, e);
                        throw new KafkaStorageException(msg, e);
                    }
                    dirtyTopicIdOpt = Optional.empty();
                });
            }
        }
    }

    public PartitionMetadata read() {
        synchronized (lock) {
            try {
                try (BufferedReader reader = Files.newBufferedReader(path(), StandardCharsets.UTF_8)) {
                    PartitionMetadataReadBuffer partitionBuffer = new PartitionMetadataReadBuffer(file.getAbsolutePath(), reader);
                    return partitionBuffer.read();
                }
            } catch (IOException e) {
                String msg = "Error while reading partition metadata file " + file.getAbsolutePath();
                logDirFailureChannel.maybeAddOfflineLogDir(logDir(), msg, e);
                throw new KafkaStorageException(msg, e);
            }
        }
    }

    public boolean exists() {
        return file.exists();
    }

    public void delete() throws IOException {
        Files.delete(file.toPath());
    }

    private Path path() {
        return file.toPath().toAbsolutePath();
    }

    private Path tempPath() {
        return Paths.get(path() + ".tmp");
    }

    private String logDir() {
        return file.getParentFile().getParent();
    }

    @Override
    public String toString() {
        return "PartitionMetadataFile(" +
            "path=" + path() +
            ')';
    }
}
