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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.OffsetAndEpoch;

public final class FileSnapshotWriter implements SnapshotWriter {
    private final Path path;
    private final FileChannel channel;
    private final OffsetAndEpoch snapshotId;
    private boolean frozen = false;

    private FileSnapshotWriter(
        Path path,
        FileChannel channel,
        OffsetAndEpoch snapshotId
    ) {
        this.path = path;
        this.channel = channel;
        this.snapshotId = snapshotId;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long sizeInBytes() throws IOException {
        return channel.size();
    }

    @Override
    public void append(ByteBuffer buffer) throws IOException {
        if (frozen) {
            throw new IllegalStateException(
                String.format("Append not supported. Snapshot is already frozen: id = %s; path = %s", snapshotId, path)
            );
        }

        Utils.writeFully(channel, buffer);
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public void freeze() throws IOException {
        channel.close();
        frozen = true;

        // Set readonly and ignore the result
        if (!path.toFile().setReadOnly()) {
            throw new IOException(String.format("Unable to set file %s as read-only", path));
        }

        Path destination = Snapshots.moveRename(path, snapshotId);
        Files.move(path, destination, StandardCopyOption.ATOMIC_MOVE);
    }

    @Override
    public void close() throws IOException {
        channel.close();
        Files.deleteIfExists(path);
    }

    public static FileSnapshotWriter create(Path logDir, OffsetAndEpoch snapshotId) throws IOException {
        Path path = Snapshots.createTempFile(logDir, snapshotId);

        return new FileSnapshotWriter(
            path,
            FileChannel.open(path, Utils.mkSet(StandardOpenOption.WRITE, StandardOpenOption.APPEND)),
            snapshotId
        );
    }
}
