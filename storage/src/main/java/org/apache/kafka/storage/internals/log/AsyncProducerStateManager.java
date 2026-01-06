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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.metadata.storage.generated.ProducerSnapshot;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

public class AsyncProducerStateManager extends ProducerStateManager {
    private static final ConcurrentSkipListMap<Long, SnapshotFile> EMPTY_SNAPSHOT_FILES = new ConcurrentSkipListMap<>();

    private final MetadataStoreExtended metadataStore;

    public AsyncProducerStateManager(TopicPartition topicPartition, int maxTransactionTimeoutMs,
                                     ProducerStateManagerConfig producerStateManagerConfig, Time time,
                                     MetadataStoreExtended metadataStore) throws IOException {
        super(topicPartition, null, maxTransactionTimeoutMs, producerStateManagerConfig, time);
        this.metadataStore = metadataStore;
    }

    @Override
    protected ConcurrentSkipListMap<Long, SnapshotFile> loadSnapshots() throws IOException {
        return EMPTY_SNAPSHOT_FILES;
    }

    @Override
    public void truncateAndReload(long logStartOffset, long logEndOffset, long currentTimeMs) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void truncateFullyAndReloadSnapshots() throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void truncateFullyAndStartAt(long offset) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void removeStraySnapshots(Collection<Long> segmentBaseOffsets) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Optional<File> takeSnapshot(boolean sync) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void takeSnapshot() throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void updateParentDir(File parentDir) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public OptionalLong latestSnapshotOffset() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public OptionalLong oldestSnapshotOffset() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Optional<SnapshotFile> snapshotFileForOffset(long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void onLogStartOffsetIncremented(long logStartOffset) {
        // todo
    }

    @Override
    public void deleteSnapshotsBefore(long offset) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Optional<File> fetchSnapshot(long offset) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Optional<SnapshotFile> removeAndMarkSnapshotForDeletion(long snapshotOffset) throws IOException {
        throw new UnsupportedOperationException("Not supported");
    }

    public CompletableFuture<Void> takeSnapshotAsync() {
        if (lastSnapOffset <= lastMapOffset) {
            return CompletableFuture.completedFuture(null);
        }

        String key = buildSnapshotKey();
        long lastMapOffset0 = lastMapOffset;
        List<ProducerSnapshot.ProducerEntry> producerEntries = new ArrayList<>(producers.size());
        for (Map.Entry<Long, ProducerStateEntry> producerIdEntry : producers.entrySet()) {
            Long producerId = producerIdEntry.getKey();
            ProducerStateEntry entry = producerIdEntry.getValue();
            producerEntries.add(new ProducerSnapshot.ProducerEntry()
                    .setProducerId(producerId)
                    .setEpoch(entry.producerEpoch())
                    .setLastSequence(entry.lastSeq())
                    .setLastOffset(entry.lastDataOffset())
                    .setOffsetDelta(entry.lastOffsetDelta())
                    .setTimestamp(entry.lastTimestamp())
                    .setCoordinatorEpoch(entry.coordinatorEpoch())
                    .setCurrentTxnFirstOffset(entry.currentTxnFirstOffset().orElse(-1L)));
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ProducerStateSnapshot snapshot = new ProducerStateSnapshot(topicPartition, lastMapOffset0, producerEntries);
        byte[] snapshotData;
        try {
            snapshotData = snapshot.toByteArray();
        } catch (Throwable t) {
            log.error("Failed to serialize snapshot", t);
            future.completeExceptionally(t);
            return future;
        }

        metadataStore.put(key, snapshotData, Optional.empty())
                .thenAccept(v -> {
                    log.info("Snapshot for topic partition {} completed", topicPartition);
                    lastSnapOffset = lastMapOffset0;
                    future.complete(null);
                })
                .exceptionally(t -> {
                    log.error("Failed to take snapshot for topic partition {}", topicPartition, t);
                    future.completeExceptionally(t);
                    return null;
                });
        return future;
    }

    public CompletableFuture<Void> recoverSnapshotAsync() {
        String key = buildSnapshotKey();
        CompletableFuture<Optional<GetResult>> snapshotFuture = metadataStore.get(key);
        CompletableFuture<Void> future = new CompletableFuture<>();

        snapshotFuture
                .thenAccept(opt -> {
                    if (opt.isEmpty()) {
                        log.info("No snapshot found for topic partition {}", topicPartition);
                        future.completeExceptionally(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception("Cannot load topic-partition ProducerSnapshot"));
                        return;
                    }
                    byte[] snapshotData = opt.get().getValue();
                    try {
                        ProducerStateSnapshot snapshot = ProducerStateSnapshot.fromByteArray(snapshotData);
                        this.lastMapOffset = snapshot.lastMapOffset;
                        for (ProducerSnapshot.ProducerEntry entry : snapshot.producerEntries) {
                            OptionalLong currentTxnFirstOffsetVal = entry.currentTxnFirstOffset() >= 0 ? OptionalLong.of(entry.currentTxnFirstOffset()) : OptionalLong.empty();
                            loadProducerEntry(new ProducerStateEntry(entry.producerId(), entry.epoch(), entry.coordinatorEpoch(), entry.timestamp(), currentTxnFirstOffsetVal));
                        }
                    } catch (Throwable t) {
                        log.error("Failed to deserialize snapshot", t);
                        future.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception(t.getMessage()));
                    }
                })
                .exceptionally(t -> {
                    Throwable root = ExceptionUtils.getRootCause(t);
                    if (root instanceof MetadataStoreException.NotFoundException) {
                        future.complete(null);
                    } else {
                        log.error("Failed to load topic-partition ProducerSnapshot for topic partition {}", topicPartition, t);
                        future.completeExceptionally(Errors.KAFKA_STORAGE_ERROR.exception(root.getMessage()));
                    }
                    return null;
                });

        return future;
    }

    private String buildSnapshotKey() {
        return String.format("/kafka/psm/%s-%s", topicPartition.topic(), topicPartition.partition());
    }


    @VisibleForTesting
    public static class ProducerStateSnapshot {
        private final TopicPartition topicPartition;
        private final long lastMapOffset;
        private final List<ProducerSnapshot.ProducerEntry> producerEntries;

        // 序列化版本，用于向后兼容
        private static final short CURRENT_VERSION = 1;

        public ProducerStateSnapshot(TopicPartition topicPartition, long lastMapOffset,
                                     List<ProducerSnapshot.ProducerEntry> producerEntries) {
            this.topicPartition = topicPartition;
            this.lastMapOffset = lastMapOffset;
            this.producerEntries = producerEntries;
        }

        public TopicPartition topicPartition() {
            return topicPartition;
        }

        public long lastMapOffset() {
            return lastMapOffset;
        }

        public List<ProducerSnapshot.ProducerEntry> producerEntries() {
            return producerEntries;
        }

        public byte[] toByteArray() throws IOException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 DataOutputStream out = new DataOutputStream(baos)) {
                out.writeShort(CURRENT_VERSION);
                writeTopicPartition(out, topicPartition);
                out.writeLong(lastMapOffset);
                writeProducerEntries(out, producerEntries);
                out.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize ProducerStateSnapshot", e);
            }
        }

        private void writeTopicPartition(DataOutputStream out, TopicPartition tp) throws IOException {
            writeString(out, tp.topic());
            out.writeInt(tp.partition());
        }

        private void writeProducerEntries(DataOutputStream out,
                                          List<ProducerSnapshot.ProducerEntry> entries) throws IOException {
            out.writeInt(entries.size());
            for (ProducerSnapshot.ProducerEntry entry : entries) {
                writeProducerEntry(out, entry);
            }
        }

        private void writeProducerEntry(DataOutputStream out,
                                        ProducerSnapshot.ProducerEntry entry) throws IOException {
            out.writeLong(entry.producerId());
            out.writeShort(entry.epoch());
            out.writeInt(entry.lastSequence());
            out.writeLong(entry.lastOffset());
            out.writeInt(entry.offsetDelta());
            out.writeLong(entry.timestamp());
            out.writeInt(entry.coordinatorEpoch());
            out.writeBoolean(true);
            out.writeLong(entry.currentTxnFirstOffset());
        }

        private void writeString(DataOutputStream out, String str) throws IOException {
            if (str == null) {
                out.writeInt(-1);
                return;
            }
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }


        public static ProducerStateSnapshot fromByteArray(byte[] data) {
            try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
                short version = in.readShort();
                if (version != CURRENT_VERSION) {
                    throw new IllegalArgumentException("Unsupported version: " + version);
                }
                TopicPartition topicPartition = readTopicPartition(in);
                long lastMapOffset = in.readLong();
                List<ProducerSnapshot.ProducerEntry> entries = readProducerEntries(in);
                return new ProducerStateSnapshot(topicPartition, lastMapOffset, entries);
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize ProducerStateSnapshot", e);
            }
        }

        private static TopicPartition readTopicPartition(DataInputStream in) throws IOException {
            String topic = readString(in);
            int partition = in.readInt();
            return new TopicPartition(topic, partition);
        }

        private static String readString(DataInputStream in) throws IOException {
            int length = in.readInt();
            if (length == -1) {
                return null;
            }

            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private static List<ProducerSnapshot.ProducerEntry> readProducerEntries(DataInputStream in)
                throws IOException {
            int count = in.readInt();
            List<ProducerSnapshot.ProducerEntry> entries = new ArrayList<>(count);

            for (int i = 0; i < count; i++) {
                entries.add(readProducerEntry(in));
            }

            return entries;
        }

        private static ProducerSnapshot.ProducerEntry readProducerEntry(DataInputStream in)
                throws IOException {
            long producerId = in.readLong();
            short producerEpoch = in.readShort();
            int lastSeq = in.readInt();
            long lastOffset = in.readLong();
            int offsetDelta = in.readInt();
            long timestamp = in.readLong();
            int coordinatorEpoch = in.readInt();

            Optional<Long> currentTxnFirstOffset;
            if (in.readBoolean()) {
                currentTxnFirstOffset = Optional.of(in.readLong());
            } else {
                currentTxnFirstOffset = Optional.of(-1L);
            }

            return new ProducerSnapshot.ProducerEntry()
                    .setProducerId(producerId)
                    .setEpoch(producerEpoch)
                    .setLastSequence(lastSeq)
                    .setLastOffset(lastOffset)
                    .setOffsetDelta(offsetDelta)
                    .setTimestamp(timestamp)
                    .setCoordinatorEpoch(coordinatorEpoch)
                    .setCurrentTxnFirstOffset(currentTxnFirstOffset.get());
        }
    }
}
