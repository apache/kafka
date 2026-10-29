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

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Result metadata of a log read operation on the log
 *
 * @param info @FetchDataInfo returned by the @Log read
 * @param divergingEpoch Optional epoch and end offset which indicates the largest epoch such
 *                       that subsequent records are known to diverge on the follower/consumer
 * @param highWatermark high watermark of the local replica
 * @param leaderLogStartOffset The log start offset of the leader at the time of the read
 * @param leaderLogEndOffset The log end offset of the leader at the time of the read
 * @param followerLogStartOffset The log start offset of the follower taken from the Fetch request
 * @param fetchTimeMs The time the fetch was received
 * @param lastStableOffset Current LSO or None if the result has an exception
 * @param preferredReadReplica the preferred read replica to be used for future fetches
 * @param error Errors if error encountered while reading from the log
 */
public record LogReadResult(
    FetchDataInfo info,
    Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
    long highWatermark,
    long leaderLogStartOffset,
    long leaderLogEndOffset,
    long followerLogStartOffset,
    long fetchTimeMs,
    OptionalLong lastStableOffset,
    OptionalInt preferredReadReplica,
    Errors error
) {
    public LogReadResult(
        FetchDataInfo info,
        Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
        long highWatermark,
        long leaderLogStartOffset,
        long leaderLogEndOffset,
        long followerLogStartOffset,
        long fetchTimeMs,
        OptionalLong lastStableOffset,
        Errors error) {
        this(info, divergingEpoch, highWatermark, leaderLogStartOffset, leaderLogEndOffset, followerLogStartOffset,
            fetchTimeMs, lastStableOffset, OptionalInt.empty(), error);
    }

    public LogReadResult(Errors error) {
        this(new FetchDataInfo(LogOffsetMetadata.UNKNOWN_OFFSET_METADATA, MemoryRecords.EMPTY),
            Optional.empty(),
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            UnifiedLog.UNKNOWN_OFFSET,
            -1L,
            OptionalLong.empty(),
            error);
    }

    public FetchPartitionData toFetchPartitionData(boolean isReassignmentFetch) {
        return new FetchPartitionData(
            this.error(),
            this.highWatermark,
            this.leaderLogStartOffset,
            this.info.records,
            this.divergingEpoch,
            this.lastStableOffset,
            this.info.abortedTransactions,
            this.preferredReadReplica,
            isReassignmentFetch);
    }
}
