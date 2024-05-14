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
package org.apache.kafka.snapshot;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.OffsetAndEpoch;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class FileRawSnapshotWriter implements RawSnapshotWriter {
    private final Path tempSnapshotPath;
    private final FileChannel channel;
    private final OffsetAndEpoch snapshotId;
    private long frozenSize;

    private FileRawSnapshotWriter(
        Path tempSnapshotPath,
        FileChannel channel,
        OffsetAndEpoch snapshotId
    ) {
        this.tempSnapshotPath = tempSnapshotPath;
        this.channel = channel;
        this.snapshotId = snapshotId;
        this.frozenSize = -1L;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long sizeInBytes() {
        if (frozenSize >= 0) {
            return frozenSize;
        }
        try {
            return channel.size();
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format(
                    "Error calculating snapshot size. temp path = %s, snapshotId = %s.",
                    tempSnapshotPath,
                    snapshotId),
                e
            );
        }
    }

    @Override
    public void append(UnalignedMemoryRecords records) {
        try {
            checkIfFrozen("Append");
            Utils.writeFully(channel, records.buffer());
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error writing file snapshot, " +
                    "temp path = %s, snapshotId = %s.", this.tempSnapshotPath, this.snapshotId),
                e
            );
        }
    }

    @Override
    public void append(MemoryRecords records) {
        try {
            checkIfFrozen("Append");
            Utils.writeFully(channel, records.buffer());
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error writing file snapshot, " +
                    "temp path = %s, snapshotId = %s.", this.tempSnapshotPath, this.snapshotId),
                e
            );
        }
    }

    @Override
    public boolean isFrozen() {
        return frozenSize >= 0;
    }

    @Override
    public void freeze() {
        try {
            checkIfFrozen("Freeze");

            frozenSize = channel.size();
            // force the channel to write to the file system before closing, to make sure that the file has the data
            // on disk before performing the atomic file move
            channel.force(true);
            channel.close();

            if (!tempSnapshotPath.toFile().setReadOnly()) {
                throw new IllegalStateException(String.format("Unable to set file (%s) as read-only", tempSnapshotPath));
            }

            Path destination = Snapshots.moveRename(tempSnapshotPath, snapshotId);
            Utils.atomicMoveWithFallback(tempSnapshotPath, destination);
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error freezing file snapshot, " +
                    "temp path = %s, snapshotId = %s.", this.tempSnapshotPath, this.snapshotId),
                e
            );
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
            // This is a noop if freeze was called before calling close
            Files.deleteIfExists(tempSnapshotPath);
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format("Error closing snapshot writer, " +
                    "temp path = %s, snapshotId %s.", this.tempSnapshotPath, this.snapshotId),
                e
            );
        }
    }

    @Override
    public String toString() {
        return String.format(
            "FileRawSnapshotWriter(path=%s, snapshotId=%s, frozen=%s)",
            tempSnapshotPath,
            snapshotId,
            isFrozen()
        );
    }

    void checkIfFrozen(String operation) {
        if (isFrozen()) {
            throw new IllegalStateException(
                String.format(
                    "%s is not supported. Snapshot is already frozen: id = %s; temp path = %s",
                    operation,
                    snapshotId,
                    tempSnapshotPath
                )
            );
        }
    }

    /**
     * Create a snapshot writer for topic partition log dir and snapshot id.
     *
     * @param logDir the directory for the topic partition
     * @param snapshotId the end offset and epoch for the snapshotId
     */
    public static FileRawSnapshotWriter create(Path logDir, OffsetAndEpoch snapshotId) {
        Path path = Snapshots.createTempFile(logDir, snapshotId);

        try {
            return new FileRawSnapshotWriter(
                path,
                FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.APPEND),
                snapshotId
            );
        } catch (IOException e) {
            throw new UncheckedIOException(
                String.format(
                    "Error creating snapshot writer. path = %s, snapshotId %s.",
                    path,
                    snapshotId
                ),
                e
            );
        }
    }
}
