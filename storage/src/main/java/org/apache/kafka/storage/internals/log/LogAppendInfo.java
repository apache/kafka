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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.requests.ProduceResponse.RecordError;

import java.util.Collections;
import java.util.List;
import java.util.OptionalInt;

/**
 * Struct to hold various quantities we compute about each message set before appending to the log.
 */
public class LogAppendInfo {

    public static final LogAppendInfo UNKNOWN_LOG_APPEND_INFO = new LogAppendInfo(-1, -1, OptionalInt.empty(),
            RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, -1L,
            RecordValidationStats.EMPTY, CompressionType.NONE, -1, -1L);

    private long firstOffset;
    private long lastOffset;
    private long maxTimestamp;
    private long offsetOfMaxTimestamp;
    private long logAppendTime;
    private long logStartOffset;
    private RecordValidationStats recordValidationStats;

    private final OptionalInt lastLeaderEpoch;
    private final CompressionType sourceCompression;
    private final int validBytes;
    private final long lastOffsetOfFirstBatch;
    private final List<RecordError> recordErrors;
    private final LeaderHwChange leaderHwChange;

    /**
     * Creates an instance with the given params.
     *
     * @param firstOffset            The first offset in the message set unless the message format is less than V2 and we are appending
     *                               to the follower.
     * @param lastOffset             The last offset in the message set
     * @param lastLeaderEpoch        The partition leader epoch corresponding to the last offset, if available.
     * @param maxTimestamp           The maximum timestamp of the message set.
     * @param offsetOfMaxTimestamp   The offset of the message with the maximum timestamp.
     * @param logAppendTime          The log append time (if used) of the message set, otherwise Message.NoTimestamp
     * @param logStartOffset         The start offset of the log at the time of this append.
     * @param recordValidationStats  Statistics collected during record processing, `null` if `assignOffsets` is `false`
     * @param sourceCompression      The source codec used in the message set (send by the producer)
     * @param validBytes             The number of valid bytes
     * @param lastOffsetOfFirstBatch The last offset of the first batch
     */
    public LogAppendInfo(long firstOffset,
                         long lastOffset,
                         OptionalInt lastLeaderEpoch,
                         long maxTimestamp,
                         long offsetOfMaxTimestamp,
                         long logAppendTime,
                         long logStartOffset,
                         RecordValidationStats recordValidationStats,
                         CompressionType sourceCompression,
                         int validBytes,
                         long lastOffsetOfFirstBatch) {
        this(firstOffset, lastOffset, lastLeaderEpoch, maxTimestamp, offsetOfMaxTimestamp, logAppendTime, logStartOffset,
            recordValidationStats, sourceCompression, validBytes, lastOffsetOfFirstBatch, Collections.<RecordError>emptyList(),
                LeaderHwChange.NONE);
    }

    /**
     * Creates an instance with the given params.
     *
     * @param firstOffset            The first offset in the message set unless the message format is less than V2 and we are appending
     *                               to the follower.
     * @param lastOffset             The last offset in the message set
     * @param lastLeaderEpoch        The partition leader epoch corresponding to the last offset, if available.
     * @param maxTimestamp           The maximum timestamp of the message set.
     * @param offsetOfMaxTimestamp   The offset of the message with the maximum timestamp.
     * @param logAppendTime          The log append time (if used) of the message set, otherwise Message.NoTimestamp
     * @param logStartOffset         The start offset of the log at the time of this append.
     * @param recordValidationStats  Statistics collected during record processing, `null` if `assignOffsets` is `false`
     * @param sourceCompression      The source codec used in the message set (send by the producer)
     * @param validBytes             The number of valid bytes
     * @param lastOffsetOfFirstBatch The last offset of the first batch
     * @param recordErrors           List of record errors that caused the respective batch to be dropped
     * @param leaderHwChange         Incremental if the high watermark needs to be increased after appending record
     *                               Same if high watermark is not changed. None is the default value and it means append failed
     */
    public LogAppendInfo(long firstOffset,
                         long lastOffset,
                         OptionalInt lastLeaderEpoch,
                         long maxTimestamp,
                         long offsetOfMaxTimestamp,
                         long logAppendTime,
                         long logStartOffset,
                         RecordValidationStats recordValidationStats,
                         CompressionType sourceCompression,
                         int validBytes,
                         long lastOffsetOfFirstBatch,
                         List<RecordError> recordErrors,
                         LeaderHwChange leaderHwChange) {
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.lastLeaderEpoch = lastLeaderEpoch;
        this.maxTimestamp = maxTimestamp;
        this.offsetOfMaxTimestamp = offsetOfMaxTimestamp;
        this.logAppendTime = logAppendTime;
        this.logStartOffset = logStartOffset;
        this.recordValidationStats = recordValidationStats;
        this.sourceCompression = sourceCompression;
        this.validBytes = validBytes;
        this.lastOffsetOfFirstBatch = lastOffsetOfFirstBatch;
        this.recordErrors = recordErrors;
        this.leaderHwChange = leaderHwChange;
    }

    public long firstOffset() {
        return firstOffset;
    }

    public void setFirstOffset(long firstOffset) {
        this.firstOffset = firstOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public OptionalInt lastLeaderEpoch() {
        return lastLeaderEpoch;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public long offsetOfMaxTimestamp() {
        return offsetOfMaxTimestamp;
    }

    public void setOffsetOfMaxTimestamp(long offsetOfMaxTimestamp) {
        this.offsetOfMaxTimestamp = offsetOfMaxTimestamp;
    }

    public long logAppendTime() {
        return logAppendTime;
    }

    public void setLogAppendTime(long logAppendTime) {
        this.logAppendTime = logAppendTime;
    }

    public long logStartOffset() {
        return logStartOffset;
    }

    public void setLogStartOffset(long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public RecordValidationStats recordValidationStats() {
        return recordValidationStats;
    }

    public void setRecordValidationStats(RecordValidationStats recordValidationStats) {
        this.recordValidationStats = recordValidationStats;
    }

    public CompressionType sourceCompression() {
        return sourceCompression;
    }

    public int validBytes() {
        return validBytes;
    }

    public List<RecordError> recordErrors() {
        return recordErrors;
    }

    public LeaderHwChange leaderHwChange() {
        return leaderHwChange;
    }

    /**
     * Get the first offset if it exists, else get the last offset of the first batch
     * For magic versions 2 and newer, this method will return first offset. For magic versions
     * older than 2, we use the last offset of the first batch as an approximation of the first
     * offset to avoid decompressing the data.
     */
    public long firstOrLastOffsetOfFirstBatch() {
        return firstOffset >= 0 ? firstOffset : lastOffsetOfFirstBatch;
    }

    /**
     * Get the (maximum) number of messages described by LogAppendInfo
     *
     * @return Maximum possible number of messages described by LogAppendInfo
     */
    public long numMessages() {
        if (firstOffset >= 0 && lastOffset >= 0) {
            return lastOffset - firstOffset + 1;
        }
        return 0;
    }

    /**
     * Returns a new instance containing all the fields from this instance and updated with the given LeaderHwChange value.
     *
     * @param newLeaderHwChange new value for LeaderHwChange
     * @return a new instance with the given LeaderHwChange
     */
    public LogAppendInfo copy(LeaderHwChange newLeaderHwChange) {
        return new LogAppendInfo(firstOffset, lastOffset, lastLeaderEpoch, maxTimestamp, offsetOfMaxTimestamp, logAppendTime, logStartOffset, recordValidationStats,
                sourceCompression, validBytes, lastOffsetOfFirstBatch, recordErrors, newLeaderHwChange);
    }

    public static LogAppendInfo unknownLogAppendInfoWithLogStartOffset(long logStartOffset) {
        return new LogAppendInfo(-1, -1, OptionalInt.empty(), RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
                RecordValidationStats.EMPTY, CompressionType.NONE, -1, -1L);
    }

    /**
     * In ProduceResponse V8+, we add two new fields record_errors and error_message (see KIP-467).
     * For any record failures with InvalidTimestamp or InvalidRecordException, we construct a LogAppendInfo object like the one
     * in unknownLogAppendInfoWithLogStartOffset, but with additional fields recordErrors
     */
    public static LogAppendInfo unknownLogAppendInfoWithAdditionalInfo(long logStartOffset, List<RecordError> recordErrors) {
        return new LogAppendInfo(-1, -1, OptionalInt.empty(), RecordBatch.NO_TIMESTAMP, -1L, RecordBatch.NO_TIMESTAMP, logStartOffset,
                RecordValidationStats.EMPTY, CompressionType.NONE, -1, -1L, recordErrors, LeaderHwChange.NONE);
    }

    @Override
    public String toString() {
        return "LogAppendInfo(" +
                "firstOffset=" + firstOffset +
                ", lastOffset=" + lastOffset +
                ", lastLeaderEpoch=" + lastLeaderEpoch +
                ", maxTimestamp=" + maxTimestamp +
                ", offsetOfMaxTimestamp=" + offsetOfMaxTimestamp +
                ", logAppendTime=" + logAppendTime +
                ", logStartOffset=" + logStartOffset +
                ", recordConversionStats=" + recordValidationStats +
                ", sourceCompression=" + sourceCompression +
                ", validBytes=" + validBytes +
                ", lastOffsetOfFirstBatch=" + lastOffsetOfFirstBatch +
                ", recordErrors=" + recordErrors +
                ", leaderHwChange=" + leaderHwChange +
                ')';
    }
}
