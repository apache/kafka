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

package org.apache.kafka.metadata.util;

import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.util.KRaftBatchFileReader.KRaftBatch;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.raft.internals.MemoryBatchReader;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.RecordsSnapshotReader;
import org.apache.kafka.snapshot.Snapshots;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Reads KRaft record batches from a local __cluster_metadata directory.
 */
public final class LocalDirectorySource implements ClusterMetadataSource {
    private static final Logger log = LoggerFactory.getLogger(LocalDirectorySource.class);
    private static final String LOG_SUFFIX = ".log";

    /**
     * Install the raft listener and select the initial file to read (which could be a snapshot).
     */
    class SelectInitialFile implements EventQueue.Event {
        private final RaftClient.Listener<ApiMessageAndVersion> newListener;
        private final CompletableFuture<Void> future;

        SelectInitialFile(RaftClient.Listener<ApiMessageAndVersion> newListener) {
            this.newListener = newListener;
            this.future = new CompletableFuture<>();
        }

        @Override
        public void run() throws Exception {
            listener = newListener;
            String name = selectNextFileName(true);
            if (name.isEmpty()) {
                throw new RuntimeException("Nothing to read found in " + metadataDirectory);
            }
            scheduleLoadFile(name);
            future.complete(null);
        }

        @Override
        public void handleException(Throwable e) {
            future.completeExceptionally(e);
            beginShutdown("error selecting initial file");
        }

        CompletableFuture<Void> future() {
            return future;
        }
    }

    /**
     * Select the next log file to read. This event will never pick a snapshot file, since it is assumed that we already
     * read any snapshot file we were going to read. If there are no more files to read, then shut down.
     */
    class SelectNextFile implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            String name = selectNextFileName(false);
            if (name.isEmpty()) {
                beginShutdown("done");
            } else {
                scheduleLoadFile(name);
            }
        }

        @Override
        public void handleException(Throwable e) {
            beginShutdown("error picking the next file");
        }
    }

    /**
     * Find the next file we want to read from the directory.
     *
     * @param considerSnapshots     True only if we should consider snapshot files.
     * @return                      The empty string if we should not read any more files; the file name otherwise.
     */
    private String selectNextFileName(boolean considerSnapshots) throws Exception {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(metadataDirectory))) {
            return selectNextFileName(considerSnapshots,
                    previousBaseOffset,
                    stream.iterator());
        }
    }

    /**
     * Find the next file we want to read from the directory.
     *
     * @param considerSnapshots     True only if we should consider snapshot files.
     * @param lastReadOffset        The offset of the last log file we read, or -1 if we haven't read any yet
     * @param iterator              An iterator which yields path names.
     * @return                      The empty string if we should not read any more files; the file name otherwise.
     */
    static String selectNextFileName(
        boolean considerSnapshots,
        long lastReadOffset,
        Iterator<Path> iterator
    ) {
        // If we find snapshot files and we're willing to consider them, we want to read the NEWEST snapshot file.
        // Snapshot files always take precedence over non-snapshot files. If we don't want to read snapshots (or there
        // are none) we want to read the OLDEST log file which is still newer than the last log offset we read.
        String best = "";
        long bestOffset = Long.MAX_VALUE;
        while (iterator.hasNext()) {
            Path next = iterator.next();
            String name = "" + next.getFileName();
            if (name.endsWith(LOG_SUFFIX)) {
                if (!best.endsWith(Snapshots.SUFFIX)) {
                    long nextOffset = extractLogFileOffsetFromName(name);
                    if ((nextOffset < bestOffset) && (nextOffset > lastReadOffset)) {
                        best = name;
                        bestOffset = nextOffset;
                    }
                }
            } else if (name.endsWith(Snapshots.SUFFIX)) {
                if (considerSnapshots) {
                    long nextOffset = extractSnapshotFileOffsetFromName(name);
                    if (best.endsWith(Snapshots.SUFFIX)) {
                        if (nextOffset > bestOffset) {
                            best = name;
                            bestOffset = nextOffset;
                        }
                    } else {
                        best = name;
                        bestOffset = nextOffset;
                    }
                }
            }
        }
        return best;
    }

    static long extractLogFileOffsetFromName(String name) {
        int dotIndex = name.indexOf(".");
        if (dotIndex < 0) {
            throw new RuntimeException("No dot found in .log file name.");
        }
        return Long.parseLong(name.substring(0, dotIndex));
    }

    static long extractSnapshotFileOffsetFromName(String name) {
        int dashIndex = name.indexOf("-");
        if (dashIndex < 0) {
            throw new RuntimeException("No dash found in .snapshot file name.");
        }
        return Long.parseLong(name.substring(0, dashIndex));
    }

    private void scheduleLoadFile(String name) {
        if (name.endsWith(Snapshots.SUFFIX)) {
            log.warn("Loading snapshot file " + name);
            queue.append(new LoadSnapshotFile(name));
        } else {
            log.warn("Loading log file " + name);
            queue.append(new LoadLogFile(name));
        }
    }

    class LoadSnapshotFile implements EventQueue.Event {
        private final String path;
        private final OffsetAndEpoch snapshotId;

        LoadSnapshotFile(String name) {
            this.path = metadataDirectory + File.separator + name;
            this.snapshotId = calculateSnapshotId(name);
        }

        @Override
        public void run() throws Exception {
            final AtomicReference<LeaderAndEpoch> leaderChange = new AtomicReference<>(null);
            final KRaftBatchFileReader reader = new KRaftBatchFileReader.Builder().setPath(path).build();
            listener.handleSnapshot(new RecordsSnapshotReader<>(snapshotId,
                new Iterator<Batch<ApiMessageAndVersion>>() {
                    Batch<ApiMessageAndVersion> batch = null;

                    private boolean loadNext() {
                        while (true) {
                            if (batch != null) return true;
                            if (!reader.hasNext()) return false;
                            KRaftBatch kRaftBatch = reader.next();
                            if (kRaftBatch.isControl()) {
                                Optional<LeaderAndEpoch> newLeaderChange = parseControlRecords(kRaftBatch.batch());
                                if (newLeaderChange.isPresent()) {
                                    leaderChange.set(newLeaderChange.get());
                                }
                            } else {
                                batch = kRaftBatch.batch();
                            }
                        }
                    }

                    @Override
                    public boolean hasNext() {
                        return loadNext();
                    }

                    @Override
                    public Batch<ApiMessageAndVersion> next() {
                        if (!loadNext()) throw new NoSuchElementException();
                        Batch<ApiMessageAndVersion> result = batch;
                        batch = null;
                        return result;
                    }
                },
                reader));
            if (leaderChange.get() != null) {
                listener.handleLeaderChange(leaderChange.get());
            }
            previousBaseOffset = snapshotId.offset;
            queue.append(new SelectNextFile());
        }
    }

    static OffsetAndEpoch calculateSnapshotId(String name) {
        int dashIndex = name.indexOf("-");
        if (dashIndex < 0) {
            throw new RuntimeException("Unable to find the first dash in the snapshot name " + name);
        }
        int periodIndex = name.indexOf(".");
        if (periodIndex < 0) {
            throw new RuntimeException("Unable to find the first period in the snapshot name " + name);
        }
        long offset = Long.parseLong(name.substring(0, dashIndex));
        int epoch = Integer.parseInt(name.substring(dashIndex + 1, periodIndex));
        return new OffsetAndEpoch(offset, epoch);
    }

    static Optional<LeaderAndEpoch> parseControlRecords(Batch<ApiMessageAndVersion> batch) {
        Optional<LeaderAndEpoch> leaderChange = Optional.empty();
        for (ApiMessageAndVersion messageAndVersion : batch) {
            if (messageAndVersion.message() instanceof LeaderChangeMessage) {
                LeaderChangeMessage message = (LeaderChangeMessage) messageAndVersion.message();
                if (message.leaderId() >= 0) {
                    leaderChange = Optional.of(new LeaderAndEpoch(OptionalInt.of(message.leaderId()), batch.epoch()));
                } else {
                    leaderChange = Optional.of(new LeaderAndEpoch(OptionalInt.empty(), batch.epoch()));
                }
            } else if (messageAndVersion.message() instanceof SnapshotHeaderRecord) {
                // ignore
            } else if (messageAndVersion.message() instanceof SnapshotFooterRecord) {
                // ignore
            } else {
                throw new RuntimeException("Unknown control record type " +
                        messageAndVersion.message().getClass().getCanonicalName());
            }
        }
        return leaderChange;
    }

    class LoadLogFile implements EventQueue.Event {
        private final String path;

        LoadLogFile(String name) {
            this.path = metadataDirectory + File.separator + name;
        }

        @Override
        public void run() throws Exception {
            try (KRaftBatchFileReader reader = new KRaftBatchFileReader.Builder().
                    setPath(path).
                    build()) {
                while (reader.hasNext()) {
                    KRaftBatch kraftBatch = reader.next();
                    if (kraftBatch.isControl()) {
                        Optional<LeaderAndEpoch> leaderChange = parseControlRecords(kraftBatch.batch());
                        if (leaderChange.isPresent()) {
                            listener.handleLeaderChange(leaderChange.get());
                        }
                    } else {
                        Batch<ApiMessageAndVersion> batch = kraftBatch.batch();
                        listener.handleCommit(MemoryBatchReader.of(
                                Collections.singletonList(Batch.data(
                                        batch.baseOffset(),
                                        batch.epoch(),
                                        batch.appendTimestamp(),
                                        batch.sizeInBytes(),
                                        batch.records())),
                                __ -> { }));
                    }
                    previousBaseOffset = kraftBatch.batch().baseOffset();
                }
            }
            queue.append(new SelectNextFile());
        }

        @Override
        public void handleException(Throwable e) {
            beginShutdown("error reading " + path);
        }
    }

    class Shutdown implements EventQueue.Event {
        private final String reason;

        Shutdown(String reason) {
            this.reason = reason;
        }

        @Override
        public void run() throws Exception {
            if (reason.equals("done")) {
                caughtUpFuture.complete(null);
            } else {
                caughtUpFuture.completeExceptionally(new RuntimeException(reason));
            }
            if (listener != null) {
                listener.beginShutdown();
            }
        }

        @Override
        public void handleException(Throwable e) {
            log.error("shutdown error", e);
        }
    }

    private final String metadataDirectory;
    private final KafkaEventQueue queue;
    private final CompletableFuture<Void> caughtUpFuture = new CompletableFuture<>();
    private RaftClient.Listener<ApiMessageAndVersion> listener = null;
    private long previousBaseOffset = -1;

    public LocalDirectorySource(String metadataDirectory) {
        this.metadataDirectory = metadataDirectory;
        this.queue = new KafkaEventQueue(Time.SYSTEM,
                new LogContext("[LocalDirectorySource] "), "LocalDirectorySource_");
    }

    @Override
    public void start(RaftClient.Listener<ApiMessageAndVersion> listener) throws Exception {
        SelectInitialFile event = new SelectInitialFile(listener);
        queue.append(event);
        event.future().get();
    }

    @Override
    public CompletableFuture<Void> caughtUpFuture() {
        return caughtUpFuture;
    }

    public void beginShutdown(String reason) {
        queue.beginShutdown(reason, new Shutdown(reason));
    }

    @Override
    public void close() throws Exception {
        beginShutdown("closing");
        queue.close();
    }

    @Override
    public String toString() {
        return "ClusterMetadataLogDirectoryReader(metadataDirectory=" + metadataDirectory + ")";
    }
}
