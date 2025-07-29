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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.raft.Isolation;
import org.apache.kafka.raft.LogAppendInfo;
import org.apache.kafka.raft.LogFetchInfo;
import org.apache.kafka.raft.LogOffsetMetadata;
import org.apache.kafka.raft.MetadataLogConfig;
import org.apache.kafka.raft.OffsetMetadata;
import org.apache.kafka.raft.RaftLog;
import org.apache.kafka.raft.SegmentPosition;
import org.apache.kafka.raft.ValidOffsetAndEpoch;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.snapshot.FileRawSnapshotReader;
import org.apache.kafka.snapshot.FileRawSnapshotWriter;
import org.apache.kafka.snapshot.NotifyingRawSnapshotWriter;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.RawSnapshotWriter;
import org.apache.kafka.snapshot.SnapshotPath;
import org.apache.kafka.snapshot.Snapshots;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class KafkaRaftLog implements RaftLog {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaRaftLog.class);

    private final Logger logger;
    private final UnifiedLog log;
    private final Time time;
    private final Scheduler scheduler;
    // Access to this object needs to be synchronized because it is used by the snapshotting thread to notify the
    // polling thread when snapshots are created. This object is also used to store any opened snapshot reader.
    private final TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> snapshots;
    private final TopicPartition topicPartition;
    private final MetadataLogConfig config;
    private final String logIdent;

    public KafkaRaftLog(
            UnifiedLog log,
            Time time,
            Scheduler scheduler,
            // Access to this object needs to be synchronized because it is used by the snapshotting thread to notify the
            // polling thread when snapshots are created. This object is also used to store any opened snapshot reader.
            TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> snapshots,
            TopicPartition topicPartition,
            MetadataLogConfig config,
            int nodeId) {
        this.log = log;
        this.time = time;
        this.scheduler = scheduler;
        this.snapshots = snapshots;
        this.topicPartition = topicPartition;
        this.config = config;
        this.logIdent = "[RaftLog partition=" + topicPartition + ", nodeId=" + nodeId + "] ";
        this.logger = new LogContext(logIdent).logger(KafkaRaftLog.class);
    }

    // for testing
    UnifiedLog log() {
        return log;
    }

    @Override
    public LogFetchInfo read(long startOffset, Isolation readIsolation) {
        FetchIsolation isolation = switch (readIsolation) {
            case COMMITTED -> FetchIsolation.HIGH_WATERMARK;
            case UNCOMMITTED -> FetchIsolation.LOG_END;
        };

        try {
            FetchDataInfo fetchInfo = log.read(startOffset, config.internalMaxFetchSizeInBytes(), isolation, true);
            return new LogFetchInfo(
                    fetchInfo.records,
                    new LogOffsetMetadata(
                            fetchInfo.fetchOffsetMetadata.messageOffset,
                            Optional.of(new SegmentPosition(
                                    fetchInfo.fetchOffsetMetadata.segmentBaseOffset,
                                    fetchInfo.fetchOffsetMetadata.relativePositionInSegment))
                    )
            );
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public LogAppendInfo appendAsLeader(Records records, int epoch) {
        if (records.sizeInBytes() == 0) {
            throw new IllegalArgumentException("Attempt to append an empty record set");
        }

        try {
            return handleAndConvertLogAppendInfo(log.appendAsLeader((MemoryRecords) records, epoch, AppendOrigin.RAFT_LEADER));
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public LogAppendInfo appendAsFollower(Records records, int epoch) {
        if (records.sizeInBytes() == 0) {
            throw new IllegalArgumentException("Attempt to append an empty record set");
        }

        return handleAndConvertLogAppendInfo(log.appendAsFollower((MemoryRecords) records, epoch));
    }

    private LogAppendInfo handleAndConvertLogAppendInfo(org.apache.kafka.storage.internals.log.LogAppendInfo appendInfo) {
        if (appendInfo.firstOffset() == UnifiedLog.UNKNOWN_OFFSET) {
            throw new CorruptRecordException("Append failed unexpectedly " + appendInfo);
        } else {
            return new LogAppendInfo(appendInfo.firstOffset(), appendInfo.lastOffset());
        }
    }

    @Override
    public int lastFetchedEpoch() {
        Optional<Integer> latestEpoch = log.latestEpoch();
        return latestEpoch.orElseGet(() -> latestSnapshotId().map(snapshotId -> {
            long logEndOffset = endOffset().offset();
            long startOffset = startOffset();
            if (snapshotId.offset() == startOffset && snapshotId.offset() == logEndOffset) {
                // Return the epoch of the snapshot when the log is empty
                return snapshotId.epoch();
            } else {
                throw new KafkaException(
                        "Log doesn't have a last fetch epoch and there is a snapshot (" + snapshotId + "). " +
                        "Expected the snapshot's end offset to match the log's end offset (" + logEndOffset +
                        ") and the log start offset (" + startOffset + ")"
                );
            }
        }).orElse(0));
    }

    @Override
    public OffsetAndEpoch endOffsetForEpoch(int epoch) {
        Optional<OffsetAndEpoch> endOffsetEpochOpt = log.endOffsetForEpoch(epoch);
        Optional<OffsetAndEpoch> earliestSnapshotIdOpt = earliestSnapshotId();
        if (endOffsetEpochOpt.isPresent()) {
            OffsetAndEpoch endOffsetEpoch = endOffsetEpochOpt.get();
            if (earliestSnapshotIdOpt.isPresent()) {
                OffsetAndEpoch earliestSnapshotId = earliestSnapshotIdOpt.get();
                if (endOffsetEpoch.offset() == earliestSnapshotId.offset() && endOffsetEpoch.epoch() == epoch) {
                    // The epoch is equal to the smallest epoch on the log. Override the diverging
                    // epoch to the oldest snapshot which should be the snapshot at the log start offset
                    return new OffsetAndEpoch(earliestSnapshotId.offset(), earliestSnapshotId.epoch());
                }
            }
            return new OffsetAndEpoch(endOffsetEpoch.offset(), endOffsetEpoch.epoch());
        } else {
            return new OffsetAndEpoch(endOffset().offset(), lastFetchedEpoch());
        }
    }

    @Override
    public LogOffsetMetadata endOffset() {
        org.apache.kafka.storage.internals.log.LogOffsetMetadata endOffsetMetadata = log.logEndOffsetMetadata();
        return new LogOffsetMetadata(
                endOffsetMetadata.messageOffset,
                Optional.of(new SegmentPosition(
                        endOffsetMetadata.segmentBaseOffset,
                        endOffsetMetadata.relativePositionInSegment)
                )
        );
    }

    @Override
    public long startOffset() {
        return log.logStartOffset();
    }

    @Override
    public void truncateTo(long offset) {
        long highWatermarkOffset = highWatermark().offset();
        if (offset < highWatermarkOffset) {
            throw new IllegalArgumentException("Attempt to truncate to offset " + offset +
                    ", which is below the current high watermark " + highWatermarkOffset);
        }
        log.truncateTo(offset);
    }

    @Override
    public boolean truncateToLatestSnapshot() {
        int latestEpoch = log.latestEpoch().orElse(0);
        boolean truncated = false;
        TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> forgottenSnapshots = new TreeMap<>();
        Optional<OffsetAndEpoch> snapshotIdOpt = latestSnapshotId();
        if (snapshotIdOpt.isPresent()) {
            OffsetAndEpoch snapshotId = snapshotIdOpt.get();
            if (snapshotId.epoch() > latestEpoch || (snapshotId.epoch() == latestEpoch && snapshotId.offset() > endOffset().offset())) {
                // Truncate the log fully if the latest snapshot is greater than the log end offset
                log.truncateFullyAndStartAt(snapshotId.offset(), Optional.empty());

                // Forget snapshots less than the log start offset
                synchronized (snapshots) {
                    truncated = true;
                    forgottenSnapshots = forgetSnapshotsBefore(snapshotId);
                }
            }
        }
        removeSnapshots(forgottenSnapshots, new FullTruncation());
        return truncated;
    }

    @Override
    public void initializeLeaderEpoch(int epoch) {
        log.assignEpochStartOffset(epoch, log.logEndOffset());
    }

    @Override
    public void updateHighWatermark(LogOffsetMetadata logOffsetMetadata) {
        // This API returns the new high watermark, which may be different from the passed offset
        Optional<OffsetMetadata> metadata = logOffsetMetadata.metadata();
        try {
            long logHighWatermark;
            if (metadata.isPresent() && metadata.get() instanceof SegmentPosition segmentPosition) {
                logHighWatermark = log.updateHighWatermark(
                        new org.apache.kafka.storage.internals.log.LogOffsetMetadata(
                                logOffsetMetadata.offset(),
                                segmentPosition.baseOffset(),
                                segmentPosition.relativePosition()
                        )
                );
            } else {
                logHighWatermark = log.updateHighWatermark(logOffsetMetadata.offset());
            }

            // Temporary log message until we fix KAFKA-14825
            if (logHighWatermark != logOffsetMetadata.offset()) {
                logger.warn("Log's high watermark ({}) is different from the local replica's high watermark ({})", metadata, logOffsetMetadata);
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public LogOffsetMetadata highWatermark() {
        try {
            org.apache.kafka.storage.internals.log.LogOffsetMetadata hwm = log.fetchOffsetSnapshot().highWatermark();
            Optional<OffsetMetadata> segmentPosition = !hwm.messageOffsetOnly()
                    ? Optional.of(new SegmentPosition(hwm.segmentBaseOffset, hwm.relativePositionInSegment))
                    : Optional.empty();

            return new LogOffsetMetadata(hwm.messageOffset, segmentPosition);
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    @Override
    public void flush(boolean forceFlushActiveSegment) {
        log.flush(forceFlushActiveSegment);
    }

    /**
     * Return the topic partition associated with the log.
     */
    @Override
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Return the topic ID associated with the log.
     */
    @Override
    public Uuid topicId() {
        return log.topicId().get();
    }

    @Override
    public Optional<RawSnapshotWriter> createNewSnapshot(OffsetAndEpoch snapshotId) {
        long startOffset = startOffset();
        if (snapshotId.offset() < startOffset) {
            logger.info("Cannot create a snapshot with an id ({}) less than the log start offset ({})", snapshotId, startOffset);
            return Optional.empty();
        }

        long highWatermarkOffset = highWatermark().offset();
        if (snapshotId.offset() > highWatermarkOffset) {
            throw new IllegalArgumentException(
                    "Cannot create a snapshot with an id (" + snapshotId + ") greater than the high-watermark (" + highWatermarkOffset + ")"
            );
        }

        ValidOffsetAndEpoch validOffsetAndEpoch = validateOffsetAndEpoch(snapshotId.offset(), snapshotId.epoch());
        if (validOffsetAndEpoch.kind() != ValidOffsetAndEpoch.Kind.VALID) {
            throw new IllegalArgumentException(
                    "Snapshot id (" + snapshotId + ") is not valid according to the log: " + validOffsetAndEpoch
            );
        }

        /*
          Perform a check that the requested snapshot offset is batch aligned via a log read, which
          returns the base offset of the batch that contains the requested offset. A snapshot offset
          is one greater than the last offset contained in the snapshot, and cannot go past the high
          watermark.

          This check is necessary because Raft replication code assumes the snapshot offset is the
          start of a batch. If a follower applies a non-batch aligned snapshot at offset (X) and
          fetches from this offset, the returned batch will start at offset (X - M), and the
          follower will be unable to append it since (X - M) < (X).
         */
        long baseOffset = read(snapshotId.offset(), Isolation.COMMITTED).startOffsetMetadata.offset();
        if (snapshotId.offset() != baseOffset) {
            throw new IllegalArgumentException(
                    "Cannot create snapshot at offset (" + snapshotId.offset() + ") because it is not batch aligned. " +
                    "The batch containing the requested offset has a base offset of (" + baseOffset + ")"
            );
        }
        return createNewSnapshotUnchecked(snapshotId);
    }

    @Override
    public Optional<RawSnapshotWriter> createNewSnapshotUnchecked(OffsetAndEpoch snapshotId) {
        boolean containsSnapshotId;
        synchronized (snapshots) {
            containsSnapshotId = snapshots.containsKey(snapshotId);
        }

        if (containsSnapshotId) {
            return Optional.empty();
        } else {
            return Optional.of(
                    new NotifyingRawSnapshotWriter(
                            FileRawSnapshotWriter.create(log.dir().toPath(), snapshotId),
                            this::onSnapshotFrozen
                    )
            );
        }
    }

    @Override
    public Optional<RawSnapshotReader> readSnapshot(OffsetAndEpoch snapshotId) {
        synchronized (snapshots) {
            Optional<FileRawSnapshotReader> reader = snapshots.get(snapshotId);
            if (reader == null) {
                return Optional.empty();
            } else if (reader.isPresent()) {
                return Optional.of(reader.get());
            } else {
                // Snapshot exists but has never been read before
                try {
                    FileRawSnapshotReader fileReader = FileRawSnapshotReader.open(log.dir().toPath(), snapshotId);
                    snapshots.put(snapshotId, Optional.of(fileReader));
                    return Optional.of(fileReader);
                } catch (UncheckedIOException e) {
                    if (e.getCause() instanceof NoSuchFileException) {
                        // Snapshot doesn't exist in the data dir; remove
                        Path path = Snapshots.snapshotPath(log.dir().toPath(), snapshotId);
                        logger.warn("Couldn't read {}; expected to find snapshot file {}", snapshotId, path);
                        snapshots.remove(snapshotId);
                        return Optional.empty();
                    }
                    throw e;
                }
            }
        }
    }

    @Override
    public Optional<RawSnapshotReader> latestSnapshot() {
        synchronized (snapshots) {
            return latestSnapshotId().flatMap(this::readSnapshot);
        }
    }

    @Override
    public Optional<OffsetAndEpoch> latestSnapshotId() {
        synchronized (snapshots) {
            return snapshots.isEmpty()
                    ? Optional.empty()
                    : Optional.of(snapshots.lastKey());
        }
    }

    @Override
    public Optional<OffsetAndEpoch> earliestSnapshotId() {
        synchronized (snapshots) {
            return snapshots.isEmpty()
                    ? Optional.empty()
                    : Optional.of(snapshots.firstKey());
        }
    }

    @Override
    public void onSnapshotFrozen(OffsetAndEpoch snapshotId) {
        synchronized (snapshots) {
            snapshots.put(snapshotId, Optional.empty());
        }
    }

    /**
     * Delete snapshots that come before a given snapshot ID. This is done by advancing the log start offset to the given
     * snapshot and cleaning old log segments.
     * This will only happen if the following invariants all hold true:
     *
     * <li>The given snapshot precedes the latest snapshot</li>
     * <li>The offset of the given snapshot is greater than the log start offset</li>
     * <li>The log layer can advance the offset to the given snapshot</li>
     *
     * This method is thread-safe
     */
    @Override
    public boolean deleteBeforeSnapshot(OffsetAndEpoch snapshotId) {
        try {
            return deleteBeforeSnapshot(snapshotId, new UnknownReason());
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }

    private boolean deleteBeforeSnapshot(OffsetAndEpoch snapshotId, SnapshotDeletionReason reason) throws IOException {
        boolean deleted = false;
        TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> forgottenSnapshots = new TreeMap<>();
        synchronized (snapshots) {
            Optional<OffsetAndEpoch> latestSnapshotIdOpt = latestSnapshotId();
            if (latestSnapshotIdOpt.isPresent()) {
                OffsetAndEpoch latestSnapshotId = latestSnapshotIdOpt.get();
                if (snapshots.containsKey(snapshotId) &&
                        startOffset() < snapshotId.offset() &&
                        snapshotId.offset() <= latestSnapshotId.offset() &&
                        log.maybeIncrementLogStartOffset(snapshotId.offset(), LogStartOffsetIncrementReason.SnapshotGenerated)) {
                    // Delete all segments that have a "last offset" less than the log start offset
                    int deletedSegments = log.deleteOldSegments();
                    // Remove older snapshots from the snapshots cache
                    forgottenSnapshots = forgetSnapshotsBefore(snapshotId);
                    deleted = deletedSegments != 0 || !forgottenSnapshots.isEmpty();
                }
            }
        }
        removeSnapshots(forgottenSnapshots, reason);
        return deleted;
    }

    /**
     * Force all known snapshots to have an open reader so we can know their sizes. This method is not thread-safe
     */
    private Map<OffsetAndEpoch, Long> loadSnapshotSizes() {
        Map<OffsetAndEpoch, Long> snapshotSizes = new HashMap<>();
        for (OffsetAndEpoch key : snapshots.keySet()) {
            Optional<RawSnapshotReader> snapshotReader = readSnapshot(key);
            snapshotReader.ifPresent(fileRawSnapshotReader ->
                    snapshotSizes.put(key, fileRawSnapshotReader.sizeInBytes())
            );
        }
        return snapshotSizes;
    }

    /**
     * Return the max timestamp of the first batch in a snapshot, if the snapshot exists and has records
     */
    private Optional<Long> readSnapshotTimestamp(OffsetAndEpoch snapshotId) {
        return readSnapshot(snapshotId).map(reader ->
            Snapshots.lastContainedLogTimestamp(reader, new LogContext(logIdent))
        );
    }

    /**
     * Perform cleaning of old snapshots and log segments based on size and time.
     * If our configured retention size has been violated, we perform cleaning as follows:
     *
     * <li>Find the oldest snapshot and delete it</li>
     * <li>Advance log start offset to end of next oldest snapshot</li>
     * <li>Delete log segments which wholly precede the new log start offset</li>
     *
     * This process is repeated until the retention size is no longer violated, or until only
     * a single snapshot remains.
     */
    @Override
    public boolean maybeClean() {
        synchronized (snapshots) {
            boolean didClean = false;
            try {
                didClean |= cleanSnapshotsRetentionSize();
                didClean |= cleanSnapshotsRetentionMs();
            } catch (IOException ioe) {
                throw new UncheckedIOException(ioe);
            }
            return didClean;
        }
    }

    /**
     * Iterate through the snapshots and test the given predicate to see if we should attempt to delete it. Since
     * we have some additional invariants regarding snapshots and log segments we cannot simply delete a snapshot in
     * all cases.
     *
     * <p>For the given predicate, we are testing if the snapshot identified by the first argument should be deleted.
     * The predicate returns a Some with the reason if the snapshot should be deleted and a None if the snapshot
     * should not be deleted
     */
    private boolean cleanSnapshots(Function<OffsetAndEpoch, Optional<SnapshotDeletionReason>> predicate) throws IOException {
        if (snapshots.size() < 2) {
            return false;
        }

        boolean didClean = false;
        List<OffsetAndEpoch> epoches = new ArrayList<>(snapshots.keySet());
        for (int i = 0; i < epoches.size() - 1; i++) {
            OffsetAndEpoch epoch = epoches.get(i);
            OffsetAndEpoch nextEpoch = epoches.get(i + 1);
            Optional<SnapshotDeletionReason> reason = predicate.apply(epoch);
            if (reason.isPresent()) {
                boolean deleted = deleteBeforeSnapshot(nextEpoch, reason.get());
                if (deleted) {
                    didClean = true;
                } else {
                    return didClean;
                }
            } else {
                return didClean;
            }
        }
        return didClean;
    }

    private boolean cleanSnapshotsRetentionMs() throws IOException {
        if (config.retentionMillis() < 0) {
            return false;
        }

        // Keep deleting snapshots as long as the
        Function<OffsetAndEpoch, Optional<SnapshotDeletionReason>> shouldClean = snapshotId ->
            readSnapshotTimestamp(snapshotId).flatMap(timestamp -> {
                long now = time.milliseconds();
                if (now - timestamp > config.retentionMillis()) {
                    return Optional.of(new RetentionMsBreach(now, timestamp, config.retentionMillis()));
                } else {
                    return Optional.empty();
                }
            });

        return cleanSnapshots(shouldClean);
    }

    private boolean cleanSnapshotsRetentionSize() throws IOException {
        if (config.retentionMaxBytes() < 0) {
            return false;
        }

        Map<OffsetAndEpoch, Long> snapshotSizes = loadSnapshotSizes();
        AtomicLong snapshotTotalSize = new AtomicLong(snapshotSizes.values().stream().mapToLong(Long::valueOf).sum());

        // Keep deleting snapshots and segments as long as we exceed the retention size
        Function<OffsetAndEpoch, Optional<SnapshotDeletionReason>> shouldClean = snapshotId -> {
            Long snapshotSize = snapshotSizes.get(snapshotId);
            if (snapshotSize == null) return Optional.empty();
            long logSize = log.size();
            if (logSize + snapshotTotalSize.get() > config.retentionMaxBytes()) {
                long oldSnapshotTotalSize = snapshotTotalSize.get();
                snapshotTotalSize.addAndGet(-snapshotSize);
                return Optional.of(new RetentionSizeBreach(logSize, oldSnapshotTotalSize, config.retentionMaxBytes()));
            } else {
                return Optional.empty();
            }
        };

        return cleanSnapshots(shouldClean);
    }

    /**
     * Forget the snapshots earlier than a given snapshot id and return the associated
     * snapshot readers.
     * This method assumes that the lock for `snapshots` is already held.
     */
    private TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> forgetSnapshotsBefore(OffsetAndEpoch logStartSnapshotId) {
        TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> expiredSnapshots = new TreeMap<>(snapshots.headMap(logStartSnapshotId, false));
        for (OffsetAndEpoch key : expiredSnapshots.keySet()) {
            snapshots.remove(key);
        }
        return expiredSnapshots;
    }

    /**
     * Rename the given snapshots on the log directory. Asynchronously, close and delete the
     * given snapshots after some delay.
     */
    private void removeSnapshots(
            TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> expiredSnapshots,
            SnapshotDeletionReason reason) {
        expiredSnapshots.forEach((key, value) -> {
            logger.info(reason.reason(key));
            Snapshots.markForDelete(log.dir().toPath(), key);
        });

        if (!expiredSnapshots.isEmpty()) {
            scheduler.scheduleOnce(
                    "delete-snapshot-files",
                    () -> KafkaRaftLog.deleteSnapshotFiles(log.dir().toPath(), expiredSnapshots),
                    config.internalDeleteDelayMillis()
            );
        }
    }

    @Override
    public void close() {
        log.close();
        synchronized (snapshots) {
            snapshots.values().forEach(reader -> reader.ifPresent(FileRawSnapshotReader::close));
            snapshots.clear();
        }
    }

    int snapshotCount() {
        synchronized (snapshots) {
            return snapshots.size();
        }
    }

    public static KafkaRaftLog createLog(
            TopicPartition topicPartition,
            Uuid topicId,
            File dataDir,
            Time time,
            Scheduler scheduler,
            MetadataLogConfig config,
            int nodeId) throws IOException {
        Properties props = new Properties();
        props.setProperty(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, String.valueOf(config.internalMaxBatchSizeInBytes()));
        if (config.internalSegmentBytes() != null) {
            props.setProperty(LogConfig.INTERNAL_SEGMENT_BYTES_CONFIG, String.valueOf(config.internalSegmentBytes()));
        } else {
            props.setProperty(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(config.logSegmentBytes()));
        }
        props.setProperty(TopicConfig.SEGMENT_MS_CONFIG, String.valueOf(config.logSegmentMillis()));
        props.setProperty(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, String.valueOf(ServerLogConfigs.LOG_DELETE_DELAY_MS_DEFAULT));

        // Disable time and byte retention when deleting segments
        props.setProperty(TopicConfig.RETENTION_MS_CONFIG, "-1");
        props.setProperty(TopicConfig.RETENTION_BYTES_CONFIG, "-1");
        LogConfig.validate(props);
        LogConfig defaultLogConfig = new LogConfig(props);

        if (defaultLogConfig.retentionMs >= 0) {
            throw new InvalidConfigurationException(
                    "Cannot set " + TopicConfig.RETENTION_MS_CONFIG + " above -1: " + defaultLogConfig.retentionMs
            );
        } else if (defaultLogConfig.retentionSize >= 0) {
            throw new InvalidConfigurationException(
                    "Cannot set " + TopicConfig.RETENTION_BYTES_CONFIG + " above -1: " + defaultLogConfig.retentionSize
            );
        }

        UnifiedLog log = UnifiedLog.create(
                dataDir,
                defaultLogConfig,
                0L,
                0L,
                scheduler,
                new BrokerTopicStats(),
                time,
                Integer.MAX_VALUE,
                new ProducerStateManagerConfig(Integer.MAX_VALUE, false),
                Integer.MAX_VALUE,
                new LogDirFailureChannel(5),
                false,
                Optional.of(topicId)
        );

        KafkaRaftLog metadataLog = new KafkaRaftLog(
                log,
                time,
                scheduler,
                recoverSnapshots(log),
                topicPartition,
                config,
                nodeId
        );

        if (defaultLogConfig.segmentSize() < config.logSegmentBytes()) {
            metadataLog.logger.error("Overriding {} is only supported for testing. Setting this value too low may " +
                    "lead to an inability to write batches of metadata records.",
                    MetadataLogConfig.INTERNAL_METADATA_LOG_SEGMENT_BYTES_CONFIG);
        }

        // When recovering, truncate fully if the latest snapshot is after the log end offset. This can happen to a follower
        // when the follower crashes after downloading a snapshot from the leader but before it could truncate the log fully.
        metadataLog.truncateToLatestSnapshot();

        return metadataLog;
    }

    private static TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> recoverSnapshots(UnifiedLog log) throws IOException {
        TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> snapshotsToRetain = new TreeMap<>();
        List<SnapshotPath> snapshotsToDelete = new ArrayList<>();
        String indent = "[RaftLog partition=" + log.topicPartition() + "] ";

        // Scan the log directory; deleting partial snapshots and older snapshot, only remembering immutable snapshots start
        // from logStartOffset
        try (DirectoryStream<Path> filesInDir = Files.newDirectoryStream(log.dir().toPath())) {
            filesInDir.forEach(path ->
                    Snapshots.parse(path).ifPresent(snapshotPath -> {
                        // Collect partial snapshot, deleted snapshot and older snapshot for deletion
                        if (snapshotPath.partial() || snapshotPath.deleted() || snapshotPath.snapshotId().offset() < log.logStartOffset()) {
                            snapshotsToDelete.add(snapshotPath);
                        } else {
                            snapshotsToRetain.put(snapshotPath.snapshotId(), Optional.empty());
                        }
                    })
            );

            // Before deleting any snapshots, we should ensure that the retained snapshots are
            // consistent with the current state of the log. If the log start offset is not 0,
            // then we must have a snapshot which covers the initial state up to the current
            // log start offset.
            if (log.logStartOffset() > 0) {
                Optional<OffsetAndEpoch> latestSnapshotId;
                try {
                    latestSnapshotId = Optional.ofNullable(snapshotsToRetain.lastKey());
                } catch (NoSuchElementException e) {
                    latestSnapshotId = Optional.empty();
                }
                if (latestSnapshotId.isEmpty() || latestSnapshotId.get().offset() < log.logStartOffset()) {
                    throw new IllegalStateException("Inconsistent snapshot state: there must be a snapshot " +
                            "at an offset larger then the current log start offset " + log.logStartOffset() +
                            ", but the latest snapshot is " + latestSnapshotId);
                }
            }

            for (SnapshotPath snapshotPath : snapshotsToDelete) {
                Files.deleteIfExists(snapshotPath.path());
                LOG.info("{}Deleted unneeded snapshot file with path {}", indent, snapshotPath);
            }
        }

        LOG.info("{}Initialized snapshots with IDs {} from {}", indent, snapshotsToRetain.keySet(), log.dir());
        return snapshotsToRetain;
    }

    private static void deleteSnapshotFiles(Path logDir, TreeMap<OffsetAndEpoch, Optional<FileRawSnapshotReader>> expiredSnapshots) {
        expiredSnapshots.forEach((snapshotId, snapshotReader) -> {
            snapshotReader.ifPresent(reader -> Utils.closeQuietly(reader, "FileRawSnapshotReader"));
            Snapshots.deleteIfExists(logDir, snapshotId);
        });
    }

    interface SnapshotDeletionReason {
        String reason(OffsetAndEpoch snapshotId);
    }

    record RetentionMsBreach(long now, long timestamp, long retentionMillis) implements SnapshotDeletionReason {
        @Override
        public String reason(OffsetAndEpoch snapshotId) {
            return "Marking snapshot " + snapshotId + " for deletion because its timestamp (" + timestamp + ") is now (" +
                    now + ") older than the retention (" + retentionMillis + ")";
        }
    }

    record RetentionSizeBreach(long logSize, long snapshotsSize, long retentionMaxBytes) implements SnapshotDeletionReason {
        @Override
        public String reason(OffsetAndEpoch snapshotId) {
            return "Marking snapshot " + snapshotId + " for deletion because the log size (" + logSize + ") and snapshots size (" +
                    snapshotsSize + ") is greater than " + retentionMaxBytes;
        }
    }

    record FullTruncation() implements SnapshotDeletionReason {
        @Override
        public String reason(OffsetAndEpoch snapshotId) {
            return "Marking snapshot " + snapshotId + " for deletion because the partition was fully truncated";
        }
    }

    record UnknownReason() implements SnapshotDeletionReason {
        @Override
        public String reason(OffsetAndEpoch snapshotId) {
            return "Marking snapshot " + snapshotId + " for deletion for unknown reason";
        }
    }
}
