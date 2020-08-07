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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

/**
 * A no-op replicated state machine  who accepts whatever records produced,
 * without data validation on the sequence order.
 */
public class NoOpStateMachine implements ReplicatedStateMachine {

    private final StringDeserializer stringDeserializer = new StringDeserializer();

    private final int nodeId;
    private final TopicPartition partition;
    private final AckMode ackMode;
    private final boolean verbose;

    private OffsetAndEpoch position = new OffsetAndEpoch(0, 0);
    private RecordAppender appender = null;
    private OptionalInt leaderId = OptionalInt.empty();

    public NoOpStateMachine(int nodeId,
                            TopicPartition partition,
                            AckMode ackMode,
                            boolean verbose) {
        this.nodeId = nodeId;
        this.partition = partition;
        this.ackMode = ackMode;
        this.verbose = verbose;
    }

    @Override
    public synchronized boolean accept(Records records) {
        return true;
    }

    @Override
    public void initialize(RecordAppender recordAppender) {
        appender = recordAppender;
    }

    @Override
    public synchronized void becomeLeader(int epoch) {
        leaderId = OptionalInt.of(nodeId);
    }

    @Override
    public synchronized void becomeFollower(int epoch, int newLeaderId) {
        leaderId = OptionalInt.of(newLeaderId);
    }

    @Override
    public synchronized OffsetAndEpoch position() {
        return position;
    }

    /**
     * Get the current leader id, or -1 if not defined.
     */
    public synchronized int leaderId() {
        return leaderId.orElse(-1);
    }

    @Override
    public synchronized void apply(Records records) {
        for (RecordBatch batch : records.batches()) {
            if (verbose) {
                for (Record record : batch) {
                    System.out.println(stringDeserializer.deserialize(partition.topic(),
                        Utils.toArray(record.value())));
                }
            }
            position = new OffsetAndEpoch(batch.lastOffset() + 1, batch.partitionLeaderEpoch());
        }
    }

    public synchronized CompletableFuture<Integer> appendRecords(Records records) {
        if (appender == null) {
            throw new IllegalStateException("The record appender is not initialized");
        }

        return appender.append(records, ackMode).thenApply(offsetAndEpoch -> records.sizeInBytes());
    }
}
