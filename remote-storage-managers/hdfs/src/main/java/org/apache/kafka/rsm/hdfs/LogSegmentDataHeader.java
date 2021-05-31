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
package org.apache.kafka.rsm.hdfs;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.EnumMap;
import java.util.Objects;

public class LogSegmentDataHeader {

    public enum FileType {
        // NOTE: DONT CHANGE THE ORDER OF THE FILE TYPES. Once a new file type is added, make sure to change the version
        // and handle the SERDE for backward compatibility.
        OFFSET_INDEX((byte) 0),
        TIMESTAMP_INDEX((byte) 1),
        LEADER_EPOCH_CHECKPOINT((byte) 2),
        PRODUCER_SNAPSHOT((byte) 3),
        TRANSACTION_INDEX((byte) 4),
        SEGMENT((byte) 5);

        private final byte id;

        FileType(byte id) {
            this.id = id;
        }

        public static FileType fromId(byte id) {
            switch (id) {
                case 0:
                    return OFFSET_INDEX;
                case 1:
                    return TIMESTAMP_INDEX;
                case 2:
                    return LEADER_EPOCH_CHECKPOINT;
                case 3:
                    return PRODUCER_SNAPSHOT;
                case 4:
                    return TRANSACTION_INDEX;
                case 5:
                    return SEGMENT;
                default:
                    return null;
            }
        }
    }

    public static final Integer LENGTH = 25;
    public static final byte CURRENT_VERSION = 0;

    private byte version;
    private final EnumMap<FileType, Integer> filePositions = new EnumMap<>(FileType.class);

    private LogSegmentDataHeader() {
    }

    @VisibleForTesting
    byte version() {
        return version;
    }

    @VisibleForTesting
    EnumMap<FileType, Integer> filePositions() {
        return filePositions;
    }

    public DataPosition getDataPosition(final FileType fileType) {
        final Integer position = filePositions.get(fileType);
        switch (fileType) {
            case OFFSET_INDEX:
            case TIMESTAMP_INDEX:
            case LEADER_EPOCH_CHECKPOINT:
            case PRODUCER_SNAPSHOT:
            case TRANSACTION_INDEX:
                final FileType nextFileType = FileType.fromId((byte) (fileType.id + 1));
                final int nextFilePosition = filePositions.get(nextFileType);
                final int length = nextFilePosition - position;
                return new DataPosition(position, length);
            case SEGMENT:
                return new DataPosition(position, Integer.MAX_VALUE);
            default:
                throw new IllegalArgumentException(String.format("FileType %s is invalid", fileType));
        }
    }

    @Override
    public String toString() {
        return "LogSegmentDataHeader{" +
                "version=" + version +
                ", filePositions=" + filePositions +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogSegmentDataHeader that = (LogSegmentDataHeader) o;
        return version == that.version && Objects.equals(filePositions, that.filePositions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, filePositions);
    }

    public static byte[] serialize(final LogSegmentDataHeader header) {
        final byte[] buf = new byte[LENGTH];
        final ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        byteBuffer.put(header.version);
        for (final FileType fileType : FileType.values()) {
            byteBuffer.putInt(header.filePositions.get(fileType));
        }
        return buf;
    }

    public static LogSegmentDataHeader deserialize(final ByteBuffer buffer) {
        final LogSegmentDataHeader dataHeader = new LogSegmentDataHeader();
        dataHeader.version = buffer.get();
        if (dataHeader.version != CURRENT_VERSION) {
            throw new UnsupportedVersionException("Unsupported version!");
        }
        for (final FileType fileType : FileType.values()) {
            dataHeader.filePositions.put(fileType, buffer.getInt());
        }
        return dataHeader;
    }

    public static LogSegmentDataHeader create(final LogSegmentData segmentData) {
        final LogSegmentDataHeader header = new LogSegmentDataHeader();
        header.version = CURRENT_VERSION;
        int startPos = LENGTH;
        for (final FileType fileType : FileType.values()) {
            long length = getIndexLength(segmentData, fileType);
            header.filePositions.put(fileType, startPos);
            startPos += (int) length;
        }
        return header;
    }

    private static long getIndexLength(final LogSegmentData segmentData,
                                       final FileType type) {
        switch (type) {
            case OFFSET_INDEX:
                return fileLength(segmentData.offsetIndex());
            case TIMESTAMP_INDEX:
                return fileLength(segmentData.timeIndex());
            case LEADER_EPOCH_CHECKPOINT:
                return segmentData.leaderEpochIndex().limit();
            case PRODUCER_SNAPSHOT:
                return fileLength(segmentData.producerSnapshotIndex());
            case TRANSACTION_INDEX:
                return fileLength(segmentData.transactionIndex().orElse(null));
            case SEGMENT:
                return fileLength(segmentData.logSegment());
            default:
                return 0L;
        }
    }

    private static long fileLength(Path path) {
        return (path != null) ? path.toFile().length() : 0L;
    }

    public static class DataPosition {
        private final int pos;
        private final int length;

        public DataPosition(final int pos, final int length) {
            this.pos = pos;
            this.length = length;
        }

        public int getPos() {
            return pos;
        }

        public int getLength() {
            return length;
        }

        @Override
        public String toString() {
            return "DataPosition{" +
                    "pos=" + pos +
                    ", length=" + length +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataPosition that = (DataPosition) o;
            return pos == that.pos && length == that.length;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pos, length);
        }
    }
}
