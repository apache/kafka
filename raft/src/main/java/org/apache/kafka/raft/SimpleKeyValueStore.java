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
package org.apache.kafka.raft;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * This is an experimental key-value store built on top of Raft consensus. Really
 * just trying to figure out what a useful API looks like.
 */
public class SimpleKeyValueStore<K, V> implements ReplicatedStateMachine {
    private RecordAppender appender = null;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final Map<K, V> committed = new HashMap<>();
    private final Map<K, V> uncommitted = new HashMap<>();
    private OffsetAndEpoch currentPosition = new OffsetAndEpoch(0L, 0);
    private SortedMap<OffsetAndEpoch, CompletableFuture<OffsetAndEpoch>> pendingCommit = new TreeMap<>();

    public SimpleKeyValueStore(Serde<K> keySerde,
                               Serde<V> valueSerde) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    public synchronized V get(K key) {
        return committed.get(key);
    }

    public synchronized CompletableFuture<OffsetAndEpoch> put(K key, V value) {
        return putAll(Collections.singletonMap(key, value));
    }

    public synchronized CompletableFuture<OffsetAndEpoch> putAll(Map<K, V> map) {
        // Append returns after the data was accepted by the leader, but we need to wait
        // for it to be committed.
        if (appender == null) {
            throw new IllegalStateException("The record appender is not initialized");
        }

        CompletableFuture<OffsetAndEpoch> appendFuture = appender.append(buildRecords(map));
        return appendFuture.thenCompose(offsetAndEpoch -> {
            synchronized (this) {
                // It is possible when this is invoked that the operation has already been applied to
                // the state machine
                if (offsetAndEpoch.compareTo(currentPosition) < 0) {
                    return CompletableFuture.completedFuture(offsetAndEpoch);
                } else {
                    CompletableFuture<OffsetAndEpoch> commitFuture = new CompletableFuture<>();
                    pendingCommit.put(offsetAndEpoch, commitFuture);
                    return commitFuture;
                }
            }
        });
    }

    @Override
    public void initialize(RecordAppender recordAppender) {
        this.appender = recordAppender;
    }

    @Override
    public synchronized void becomeLeader(int epoch) {
    }

    @Override
    public synchronized void becomeFollower(int epoch) {
        uncommitted.clear();
    }

    @Override
    public synchronized OffsetAndEpoch position() {
        return currentPosition;
    }

    @Override
    public synchronized void apply(Records records) {
        withRecords(records, (key, value) -> {
            uncommitted.remove(key, value);
            committed.put(key, value);
        });

        for (RecordBatch batch : records.batches()) {
            maybeCompletePendingCommit(batch);
            currentPosition = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    @Override
    public synchronized boolean accept(Records records) {
        withRecords(records, uncommitted::put);
        return true;
    }

    private void withRecords(Records records, BiConsumer<K, V> action) {
        for (RecordBatch batch : records.batches()) {
            if (batch.isControlBatch()) {
                continue;
            }
            for (Record record : batch) {
                byte[] keyBytes = Utils.toArray(record.key());
                byte[] valueBytes = Utils.toArray(record.value());

                K key = keySerde.deserializer().deserialize(null, keyBytes);
                V value = valueSerde.deserializer().deserialize(null, valueBytes);

                action.accept(key, value);
            }
        }
    }

    private Records buildRecords(Map<K, V> map) {
        SimpleRecord[] records = new SimpleRecord[map.size()];
        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            records[i++] = serialize(entry.getKey(), entry.getValue());
        }
        return MemoryRecords.withRecords(CompressionType.NONE, records);
    }

    private SimpleRecord serialize(K key, V value) {
        byte[] keyBytes = keySerde.serializer().serialize(null, key);
        byte[] valueBytes = valueSerde.serializer().serialize(null, value);
        return new SimpleRecord(keyBytes, valueBytes);
    }

    private void maybeCompletePendingCommit(RecordBatch batch) {
        Iterator<Map.Entry<OffsetAndEpoch, CompletableFuture<OffsetAndEpoch>>> iter =
                pendingCommit.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<OffsetAndEpoch, CompletableFuture<OffsetAndEpoch>> entry = iter.next();
            CompletableFuture<OffsetAndEpoch> future = entry.getValue();
            OffsetAndEpoch offsetAndEpoch = entry.getKey();
            if (future.isCompletedExceptionally() || future.isCancelled()) {
                iter.remove();
            } else if (batch.partitionLeaderEpoch() > offsetAndEpoch.epoch) {
                future.completeExceptionally(new LogTruncationException("The log was truncated " +
                        "before the record was committed"));
                iter.remove();
            } else if (batch.lastOffset() >= offsetAndEpoch.offset) {
                // comparing the last offset of the appended future with the batch last offset,
                // if it is smaller then we know the appended records are replicated completely
                future.complete(offsetAndEpoch);
                iter.remove();
            } else {
                break;
            }
        }
    }

    @Override
    public void close() {
        uncommitted.clear();
        committed.clear();
        pendingCommit.clear();
    }
}
