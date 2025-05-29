package org.apache.kafka.server;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.FetchDataInfo;

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
 * @param exception Exception if error encountered while reading from the log
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
    Optional<Throwable> exception
) {
    public LogReadResult(
            FetchDataInfo info,
            Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
            long highWatermark,
            long leaderLogStartOffset,
            long leaderLogEndOffset,
            long followerLogStartOffset,
            long fetchTimeMs,
            OptionalLong lastStableOffset) {
        this(info, divergingEpoch, highWatermark, leaderLogStartOffset, leaderLogEndOffset, followerLogStartOffset,
            fetchTimeMs, lastStableOffset, OptionalInt.empty(), Optional.empty());
    }

    public LogReadResult(
        FetchDataInfo info,
        Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
        long highWatermark,
        long leaderLogStartOffset,
        long leaderLogEndOffset,
        long followerLogStartOffset,
        long fetchTimeMs,
        OptionalLong lastStableOffset,
        Optional<Throwable> exception) {
        this(info, divergingEpoch, highWatermark, leaderLogStartOffset, leaderLogEndOffset, followerLogStartOffset,
            fetchTimeMs, lastStableOffset, OptionalInt.empty(), exception);
    }

    public LogReadResult(
            FetchDataInfo info,
            Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
            long highWatermark,
            long leaderLogStartOffset,
            long leaderLogEndOffset,
            long followerLogStartOffset,
            long fetchTimeMs,
            OptionalLong lastStableOffset,
            OptionalInt preferredReadReplica) {
        this(info, divergingEpoch, highWatermark, leaderLogStartOffset, leaderLogEndOffset, followerLogStartOffset,
            fetchTimeMs, lastStableOffset, preferredReadReplica, Optional.empty());
    }

    public Errors error() {
        if (exception.isPresent()) {
            return Errors.forException(exception.get());
        }
        return Errors.NONE;
    }

    @Override
    public String toString() {
        return "LogReadResult(info=" + info +
               ", divergingEpoch=" + divergingEpoch +
               ", highWatermark=" + highWatermark +
               ", leaderLogStartOffset" + leaderLogStartOffset +
               ", leaderLogEndOffset" + leaderLogEndOffset +
               ", followerLogStartOffset" + followerLogStartOffset +
               ", fetchTimeMs=" + fetchTimeMs +
               ", preferredReadReplica=" + preferredReadReplica +
               ", lastStableOffset=" + lastStableOffset +
               ", error=" + error() + ")";
    }

    public FetchPartitionData toFetchPartitionData(boolean isReassignmentFetch) {
        return new FetchPartitionData(
            this.error(),
            this.highWatermark,
            this.leaderLogStartOffset,
            this.info.records,
            this.divergingEpoch,
            this.lastStableOffset.isPresent() ? OptionalLong.of(this.lastStableOffset.getAsLong()) : OptionalLong.empty(),
            this.info.abortedTransactions,
            this.preferredReadReplica.isPresent() ? OptionalInt.of(this.preferredReadReplica.getAsInt()) : OptionalInt.empty(),
            isReassignmentFetch);
    }
}
