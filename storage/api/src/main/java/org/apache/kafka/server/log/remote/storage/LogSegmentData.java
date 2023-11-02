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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * This represents all the required data and indexes for a specific log segment that needs to be stored in the remote
 * storage. This is passed with {@link RemoteStorageManager#copyLogSegmentData(RemoteLogSegmentMetadata, LogSegmentData)}
 * while copying a specific log segment to the remote storage.
 */
@InterfaceStability.Evolving
public class LogSegmentData {

    private final Path logSegment;
    private final Path offsetIndex;
    private final Path timeIndex;
    private final Optional<Path> transactionIndex;
    private final Path producerSnapshotIndex;
    private final ByteBuffer leaderEpochIndex;

    /**
     * Creates a LogSegmentData instance with data and indexes.
     *
     * @param logSegment            actual log segment file
     * @param offsetIndex           offset index file
     * @param timeIndex             time index file
     * @param transactionIndex      transaction index file, which can be null
     * @param producerSnapshotIndex producer snapshot until this segment
     * @param leaderEpochIndex      leader-epoch-index until this segment
     */
    public LogSegmentData(Path logSegment,
                          Path offsetIndex,
                          Path timeIndex,
                          Optional<Path> transactionIndex,
                          Path producerSnapshotIndex,
                          ByteBuffer leaderEpochIndex) {
        this.logSegment = Objects.requireNonNull(logSegment, "logSegment can not be null");
        this.offsetIndex = Objects.requireNonNull(offsetIndex, "offsetIndex can not be null");
        this.timeIndex = Objects.requireNonNull(timeIndex, "timeIndex can not be null");
        this.transactionIndex = Objects.requireNonNull(transactionIndex, "transactionIndex can not be null");
        this.producerSnapshotIndex = Objects.requireNonNull(producerSnapshotIndex, "producerSnapshotIndex can not be null");
        this.leaderEpochIndex = Objects.requireNonNull(leaderEpochIndex, "leaderEpochIndex can not be null");
    }

    /**
     * @return Log segment file of this segment.
     */
    public Path logSegment() {
        return logSegment;
    }

    /**
     * @return Offset index file.
     */
    public Path offsetIndex() {
        return offsetIndex;
    }

    /**
     * @return Time index file of this segment.
     */
    public Path timeIndex() {
        return timeIndex;
    }

    /**
     * @return Transaction index file of this segment if it exists.
     */
    public Optional<Path> transactionIndex() {
        return transactionIndex;
    }

    /**
     * @return Producer snapshot file until this segment.
     */
    public Path producerSnapshotIndex() {
        return producerSnapshotIndex;
    }

    /**
     * @return Leader epoch index until this segment.
     */
    public ByteBuffer leaderEpochIndex() {
        return leaderEpochIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogSegmentData that = (LogSegmentData) o;
        return Objects.equals(logSegment, that.logSegment) &&
               Objects.equals(offsetIndex, that.offsetIndex) &&
               Objects.equals(timeIndex, that.timeIndex) &&
               Objects.equals(transactionIndex, that.transactionIndex) &&
               Objects.equals(producerSnapshotIndex, that.producerSnapshotIndex) &&
               Objects.equals(leaderEpochIndex, that.leaderEpochIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logSegment, offsetIndex, timeIndex, transactionIndex, producerSnapshotIndex, leaderEpochIndex);
    }

    @Override
    public String toString() {
        return "LogSegmentData{" +
               "logSegment=" + logSegment +
               ", offsetIndex=" + offsetIndex +
               ", timeIndex=" + timeIndex +
               ", txnIndex=" + transactionIndex +
               ", producerSnapshotIndex=" + producerSnapshotIndex +
               ", leaderEpochIndex=" + leaderEpochIndex +
               '}';
    }
}