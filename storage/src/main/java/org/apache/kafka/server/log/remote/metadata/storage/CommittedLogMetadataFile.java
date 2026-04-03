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
package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.metadata.storage.serialization.RemoteLogMetadataSerde;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CommittedLogMetadataFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommittedLogMetadataFile.class);
    public static final String COMMITTED_LOG_METADATA_SNAPSHOT_FILE_NAME = "remote_log_snapshot";

    // header: <version:short><topicId:2 longs><metadata-partition:int><metadata-partition-offset:long>
    // size: 2 + (8+8) + 4 + 8 = 30
    private static final int HEADER_SIZE = 30;

    private TopicIdPartition topicIdPartition;
    private final File metadataStoreFile;

    CommittedLogMetadataFile(TopicIdPartition topicIdPartition,
                             Path metadataStoreDir) {
        this.topicIdPartition = topicIdPartition;
        this.metadataStoreFile = new File(metadataStoreDir.toFile(), COMMITTED_LOG_METADATA_SNAPSHOT_FILE_NAME);

        // Create an empty file if it does not exist.
        if (!metadataStoreFile.exists()) {
            try {
                metadataStoreFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void write(Snapshot snapshot) throws IOException {
        if (!metadataStoreFile.getParentFile().exists()) {
            LOGGER.warn("Skip committing the remote log metadata snapshot for {}, as the topic got deleted",
                    topicIdPartition.topicPartition());
            return;
        }
        File newMetadataStoreFile = new File(metadataStoreFile.getAbsolutePath() + ".new");
        try (WritableByteChannel fileChannel = Channels.newChannel(new FileOutputStream(newMetadataStoreFile))) {

            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

            // Write version
            headerBuffer.putShort(snapshot.version);

            // Write topic-id
            headerBuffer.putLong(snapshot.topicId.getMostSignificantBits());
            headerBuffer.putLong(snapshot.topicId.getLeastSignificantBits());

            // Write metadata partition and metadata partition offset
            headerBuffer.putInt(snapshot.metadataPartition);
            headerBuffer.putLong(snapshot.metadataPartitionOffset);
            headerBuffer.flip();

            // Write header
            fileChannel.write(headerBuffer);

            // Write each entry
            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
            for (RemoteLogSegmentMetadata remoteLogSegmentMetadata : snapshot.remoteLogMetadatas) {
                final byte[] serializedBytes = serde.serialize(remoteLogSegmentMetadata);
                // Write length
                lenBuffer.putInt(serializedBytes.length);
                lenBuffer.flip();
                fileChannel.write(lenBuffer);
                lenBuffer.rewind();

                // Write data
                fileChannel.write(ByteBuffer.wrap(serializedBytes));
            }
        }

        Utils.atomicMoveWithFallback(newMetadataStoreFile.toPath(), metadataStoreFile.toPath());
    }

    @SuppressWarnings("unchecked")
    public synchronized Optional<Snapshot> read() throws IOException {

        // Checking for empty files.
        if (metadataStoreFile.length() == 0) {
            return Optional.empty();
        }

        try (ReadableByteChannel channel = Channels.newChannel(new FileInputStream(metadataStoreFile))) {

            // Read header
            ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);
            channel.read(headerBuffer);
            headerBuffer.rewind();
            short version = headerBuffer.getShort();
            Uuid topicId = new Uuid(headerBuffer.getLong(), headerBuffer.getLong());
            int metadataPartition = headerBuffer.getInt();
            long metadataPartitionOffset = headerBuffer.getLong();

            RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();

            List<RemoteLogSegmentMetadata> result = new ArrayList<>();

            ByteBuffer lenBuffer = ByteBuffer.allocate(4);
            while (channel.read(lenBuffer) > 0) {
                lenBuffer.rewind();
                // Read the length of each entry
                final int len = lenBuffer.getInt();
                lenBuffer.rewind();

                // Read the entry
                ByteBuffer data = ByteBuffer.allocate(len);
                final int read = channel.read(data);
                if (read != len) {
                    throw new IOException("Invalid amount of data read, file may have been corrupted.");
                }

                // We are always adding RemoteLogSegmentMetadata only as you can see in #write() method.
                // Did not add a specific serde for RemoteLogSegmentMetadata and reusing RemoteLogMetadataSerde
                final RemoteLogSegmentMetadata remoteLogSegmentMetadata = (RemoteLogSegmentMetadata) serde.deserialize(data.array());
                result.add(remoteLogSegmentMetadata);
            }

            return Optional.of(new Snapshot(version, topicId, metadataPartition, metadataPartitionOffset, result));
        }
    }

    public static final class Snapshot {
        private static final short CURRENT_VERSION = 0;

        private final short version;
        private final Uuid topicId;
        private final int metadataPartition;
        private final long metadataPartitionOffset;
        private final Collection<RemoteLogSegmentMetadata> remoteLogMetadatas;

        public Snapshot(Uuid topicId,
                        int metadataPartition,
                        long metadataPartitionOffset,
                        Collection<RemoteLogSegmentMetadata> remoteLogMetadatas) {
            this(CURRENT_VERSION, topicId, metadataPartition, metadataPartitionOffset, remoteLogMetadatas);
        }

        public Snapshot(short version,
                        Uuid topicId,
                        int metadataPartition,
                        long metadataPartitionOffset,
                        Collection<RemoteLogSegmentMetadata> remoteLogMetadatas) {
            this.version = version;
            this.topicId = topicId;
            this.metadataPartition = metadataPartition;
            this.metadataPartitionOffset = metadataPartitionOffset;
            this.remoteLogMetadatas = remoteLogMetadatas;
        }

        public short version() {
            return version;
        }

        public Uuid topicId() {
            return topicId;
        }

        public int metadataPartition() {
            return metadataPartition;
        }

        public long metadataPartitionOffset() {
            return metadataPartitionOffset;
        }

        public Collection<RemoteLogSegmentMetadata> remoteLogMetadatas() {
            return remoteLogMetadatas;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Snapshot)) return false;
            Snapshot snapshot = (Snapshot) o;
            return version == snapshot.version && metadataPartition == snapshot.metadataPartition && metadataPartitionOffset == snapshot.metadataPartitionOffset && Objects
                    .equals(topicId, snapshot.topicId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, topicId, metadataPartition, metadataPartitionOffset);
        }
    }
}