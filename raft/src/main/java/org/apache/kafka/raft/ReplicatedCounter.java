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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class ReplicatedCounter {
    private final int localBrokerId;
    private final Logger log;
    private final RaftClient client;

    private final AtomicInteger committed = new AtomicInteger(0);
    private final AtomicInteger uncommitted = new AtomicInteger(0);
    private OffsetAndEpoch position = new OffsetAndEpoch(0, 0);
    private LeaderAndEpoch currentLeaderAndEpoch = new LeaderAndEpoch(OptionalInt.empty(), 0);

    public ReplicatedCounter(int localBrokerId,
                             RaftClient client,
                             LogContext logContext) {
        this.localBrokerId = localBrokerId;
        this.client = client;
        this.log = logContext.logger(ReplicatedCounter.class);
    }

    private Records tryRead(long durationMs) {
        CompletableFuture<Records> future = client.read(position, Isolation.COMMITTED, durationMs);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void apply(Record record) {
        int value = deserialize(record);
        if (value != committed.get() + 1) {
            log.debug("Ignoring non-sequential append at offset {}: {} -> {}",
                record.offset(), committed.get(), value);
            return;
        }

        log.debug("Applied increment at offset {}: {} -> {}", record.offset(), committed.get(), value);
        committed.set(value);

        if (value > uncommitted.get()) {
            uncommitted.set(value);
        }
    }

    public synchronized void poll(long durationMs) {
        // Check for leader changes
        LeaderAndEpoch latestLeaderAndEpoch = client.currentLeaderAndEpoch();
        if (!currentLeaderAndEpoch.equals(latestLeaderAndEpoch)) {
            if (localBrokerId == latestLeaderAndEpoch.leaderId.orElse(-1)) {
                uncommitted.set(committed.get());
            }
            this.currentLeaderAndEpoch = latestLeaderAndEpoch;
        }

        Records records = tryRead(durationMs);
        for (RecordBatch batch : records.batches()) {
            if (!batch.isControlBatch()) {
                batch.forEach(this::apply);
            }
            this.position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    public synchronized boolean isWritable() {
        // We only accept appends if we are the leader and we have caught up to a position
        // within the current leader epoch
        return localBrokerId == currentLeaderAndEpoch.leaderId.orElse(-1);
    }

    public synchronized void increment() {
        if (!isWritable())
            throw new KafkaException("Counter is not currently writable");
        int initialValue = uncommitted.get();
        int incrementedValue = uncommitted.incrementAndGet();
        Records records = MemoryRecords.withRecords(CompressionType.NONE, serialize(incrementedValue));
        client.append(records, AckMode.LEADER, Integer.MAX_VALUE).whenComplete((offsetAndEpoch, throwable) -> {
            if (offsetAndEpoch != null) {
                log.debug("Appended increment at offset {}: {} -> {}",
                    offsetAndEpoch.offset, initialValue, incrementedValue);
            } else {
                uncommitted.set(initialValue);
                log.debug("Failed append of increment: {} -> {}", initialValue, incrementedValue, throwable);
            }
        });
    }

    private SimpleRecord serialize(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Type.INT32.write(buffer, value);
        buffer.flip();
        return new SimpleRecord(buffer);
    }

    private int deserialize(Record record) {
        return (int) Type.INT32.read(record.value());
    }

}
