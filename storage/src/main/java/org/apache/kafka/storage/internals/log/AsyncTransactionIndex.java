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

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;

@NotThreadSafe
public class AsyncTransactionIndex {
    private static final Logger log = LoggerFactory.getLogger(AsyncTransactionIndex.class);

    private OptionalLong lastOffset = OptionalLong.empty();
    private volatile long lastSnapshotOffset = -1;
    private volatile long mapEndOffset = -1;
    private final MetadataStoreExtended metadataStore;
    private final TopicPartition tp;
    private final ConcurrentSkipListMap<Long, AbortedTxn> index;

    public AsyncTransactionIndex(MetadataStoreExtended metadataStore, TopicPartition tp) {
        this.metadataStore = metadataStore;
        this.tp = tp;
        this.index = new ConcurrentSkipListMap<>(Long::compareTo);
    }

    public void append(AbortedTxn abortedTxn, long mapEndOffset) {
        lastOffset.ifPresent(offset -> {
            if (offset >= abortedTxn.lastOffset())
                throw new IllegalArgumentException(
                        "The last offset of appended transactions must increase sequentially, " +
                                "but abortedTxn.lastOffset() " + " is not greater than current last offset " + offset);
        });
        this.mapEndOffset = mapEndOffset;
        lastOffset = OptionalLong.of(abortedTxn.lastOffset());
        index.put(abortedTxn.lastOffset(), abortedTxn);
    }

    public long mapEndOffset() {
        return mapEndOffset;
    }

    public List<AbortedTxn> allAbortedTxns() {
        return new ImmutableList.Builder<AbortedTxn>().addAll(index.values()).build();
    }

    public TxnIndexSearchResult collectAbortedTxns(long fetchOffset, long upperBoundOffset) {
        List<AbortedTxn> abortedTransactions = new ArrayList<>();
        for (AbortedTxn abortedTxn : index.tailMap(fetchOffset).values()) {
            if (abortedTxn.lastOffset() >= fetchOffset && abortedTxn.firstOffset() < upperBoundOffset)
                abortedTransactions.add(abortedTxn);

            if (abortedTxn.lastStableOffset() >= upperBoundOffset)
                return new TxnIndexSearchResult(abortedTransactions, true);
        }
        return new TxnIndexSearchResult(abortedTransactions, false);
    }


    public void onLogStartOffsetChanged(long logStartOffset) {
        index.headMap(logStartOffset).clear();
    }

    public CompletableFuture<Void> takeSnapshot() {
        if (lastSnapshotOffset <= mapEndOffset) {
            return CompletableFuture.completedFuture(null);
        }

        long lastOffset = this.lastOffset.orElse(-1L);
        long mapEndOffset = this.mapEndOffset;
        byte[] snapshot = new TransactionIndexSnapshot(tp, lastOffset, mapEndOffset, index.values()).toByteArray();
        return metadataStore.put(buildKey(), snapshot, Optional.empty())
                .thenApply(ignore -> {
                    lastSnapshotOffset = mapEndOffset;
                    return null;
                });
    }


    private String buildKey() {
        return String.format("/kafka/txn-idx/%s-%d", tp.topic(), tp.partition());
    }

    public CompletableFuture<Void> recoverSnapshot() {
        String key = buildKey();
        CompletableFuture<Void> future = new CompletableFuture<>();
        metadataStore.get(key)
                .thenAccept(opt -> {
                    if (opt.isEmpty()) {
                        future.complete(null);
                    } else {
                        TransactionIndexSnapshot snapshot = TransactionIndexSnapshot.fromByteArray(opt.get().getValue());
                        for (AbortedTxn abortedTxn : snapshot.abortedTxns()) {
                            append(abortedTxn, snapshot.mapEndOffset);
                        }
                        lastOffset = OptionalLong.of(snapshot.lastOffset());
                        lastSnapshotOffset = snapshot.lastOffset();
                        future.complete(null);
                    }
                })
                .exceptionally(t -> {
                    Throwable root = ExceptionUtils.getRootCause(t);
                    if (root instanceof MetadataStoreException.NotFoundException) {
                        future.complete(null);
                        return null;
                    }
                    log.error("Failed to recover transaction index snapshot for topic partition {}", tp, t);
                    future.completeExceptionally(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception(root.getMessage()));
                    return null;
                });
        return future;
    }


    public static class TransactionIndexSnapshot {
        private final TopicPartition tp;
        private final long lastOffset;
        private final long mapEndOffset;
        private final ByteBuf buf;
        private final Collection<AbortedTxn> abortedTxns;

        public TransactionIndexSnapshot(TopicPartition tp, long lastOffset, long mapEndOffset, Collection<AbortedTxn> abortedTxns) {
            this.tp = tp;
            this.lastOffset = lastOffset;
            this.mapEndOffset = mapEndOffset;
            this.buf = PulsarByteBufAllocator.DEFAULT.heapBuffer();
            this.abortedTxns = abortedTxns;

            byte[] topic = tp.topic().getBytes(StandardCharsets.UTF_8);
            int partition = tp.partition();
            buf.writeShort(topic.length);
            buf.writeBytes(topic);
            buf.writeInt(partition);
            buf.writeLong(lastOffset);
            buf.writeLong(mapEndOffset);
            for (AbortedTxn abortedTxn : abortedTxns) {
                ByteBuffer buffer = abortedTxn.buffer;
                buf.writeBytes(buffer);
            }
        }

        public TopicPartition topicPartition() {
            return tp;
        }

        public long lastOffset() {
            return lastOffset;
        }

        public Collection<AbortedTxn> abortedTxns() {
            return abortedTxns;
        }

        public long mapEndOffset() {
            return mapEndOffset;
        }

        public byte[] toByteArray() {
            byte[] bytes = new byte[buf.readableBytes()];
            buf.readBytes(bytes);
            return bytes;
        }

        public static TransactionIndexSnapshot fromByteArray(byte[] bytes) {
            ByteBuf buf = Unpooled.wrappedBuffer(bytes);
            short topicLength = buf.readShort();
            byte[] topicBts = new byte[topicLength];
            buf.readBytes(topicBts);
            String topic = new String(topicBts, StandardCharsets.UTF_8);
            int partition = buf.readInt();
            long lastOffset = buf.readLong();
            long mapEndOffset = buf.readLong();
            List<AbortedTxn> abortedTxns = new ArrayList<>();
            while (buf.isReadable()) {
                short version = buf.readShort();
                if (version != AbortedTxn.CURRENT_VERSION) {
                    throw new IllegalArgumentException("Unsupported version: " + version);
                }
                long producerId = buf.readLong();
                long firstOffset = buf.readLong();
                long lastOffset0 = buf.readLong();
                long lastStableOffset = buf.readLong();
                abortedTxns.add(new AbortedTxn(producerId, firstOffset, lastOffset0, lastStableOffset));
            }
            return new TransactionIndexSnapshot(new TopicPartition(topic, partition), lastOffset, mapEndOffset, abortedTxns);
        }
    }
}
