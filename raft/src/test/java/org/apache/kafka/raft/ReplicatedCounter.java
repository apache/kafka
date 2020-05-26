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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the simplest interesting state machine. It maintains a simple counter which can only
 * be incremented by one.
 */
public class ReplicatedCounter implements ReplicatedStateMachine {
    private final Logger log;
    private final int nodeId;
    private final AtomicInteger committed = new AtomicInteger(0);
    private OffsetAndEpoch position = new OffsetAndEpoch(0, 0);
    private AtomicInteger uncommitted;

    private RecordAppender appender = null;

    public ReplicatedCounter(int nodeId,
                             LogContext logContext) {
        this.nodeId = nodeId;
        this.log = logContext.logger(ReplicatedCounter.class);
    }

    @Override
    public void initialize(RecordAppender recordAppender) {
        appender = recordAppender;
    }

    @Override
    public synchronized void becomeLeader(int epoch) {
        uncommitted = new AtomicInteger(committed.get());
    }

    @Override
    public synchronized void becomeFollower(int epoch) {
        uncommitted = null;
    }

    @Override
    public synchronized OffsetAndEpoch position() {
        return position;
    }

    @Override
    public synchronized void apply(Records records) {
        for (RecordBatch batch : records.batches()) {
            if (!batch.isControlBatch()) {
                for (Record record : batch) {
                    int value = deserialize(record);

                    if (value != committed.get() + 1) {
                        throw new IllegalStateException("Node " + nodeId + " detected invalid increment in record at offset " + record.offset() +
                                                            ", epoch " + batch.partitionLeaderEpoch() + ": " + committed.get() + " -> " + value);
                    }
                    log.trace("Applied counter update at offset {}: {} -> {}", record.offset(), committed.get(), value);
                    committed.set(value);
                }
            }
            this.position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }

        if (uncommitted != null && committed.get() > uncommitted.get()) {
            uncommitted.set(committed.get());
        }
    }

    @Override
    public synchronized boolean accept(Records records) {
        int lastUncommitted = uncommitted.get();
        for (RecordBatch batch : records.batches()) {
            for (Record record : batch) {
                int value = deserialize(record);
                if (value != lastUncommitted + 1)
                    return false;
                lastUncommitted = value;
            }
        }

        log.trace("Accept counter update: {} -> {}", uncommitted.get(), lastUncommitted);
        uncommitted.set(lastUncommitted);
        return true;
    }

    public synchronized int value() {
        return committed.get();
    }

    public synchronized CompletableFuture<Integer> increment() {
        if (appender == null) {
            throw new IllegalStateException("The record appender is not initialized");
        }

        int incremented = uncommitted != null ?
            uncommitted.get() + 1 :
            value() + 1;
        Records records = MemoryRecords.withRecords(CompressionType.NONE, serialize(incremented));
        CompletableFuture<OffsetAndEpoch> future = appender.append(records);
        return future.thenApply(offsetAndEpoch -> incremented);
    }

    private SimpleRecord serialize(int value) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Type.INT32.write(buffer, value);
        buffer.flip();
        return new SimpleRecord(buffer);
    }

    private int deserialize(Record record) {
        return (Integer) Type.INT32.read(record.value());
    }

    @Override
    public void close() {
        committed.set(0);
        uncommitted = null;
        appender = null;
    }
}
