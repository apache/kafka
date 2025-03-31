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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.VerificationGuard;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An in-memory partition writer that accepts a maximum number of writes.
 */
public class MockPartitionWriter extends InMemoryPartitionWriter {
    private final Time time;
    private final int maxWrites;
    private final boolean failEndMarker;
    private final AtomicInteger writeCount = new AtomicInteger(0);

    public MockPartitionWriter() {
        this(new MockTime(), Integer.MAX_VALUE, false);
    }

    public MockPartitionWriter(int maxWrites) {
        this(new MockTime(), maxWrites, false);
    }

    public MockPartitionWriter(boolean failEndMarker) {
        this(new MockTime(), Integer.MAX_VALUE, failEndMarker);
    }

    public MockPartitionWriter(Time time, int maxWrites, boolean failEndMarker) {
        super(false);
        this.time = time;
        this.maxWrites = maxWrites;
        this.failEndMarker = failEndMarker;
    }

    @Override
    public void registerListener(TopicPartition tp, Listener listener) {
        super.registerListener(tp, listener);
    }

    @Override
    public void deregisterListener(TopicPartition tp, Listener listener) {
        super.deregisterListener(tp, listener);
    }

    @Override
    public long append(
        TopicPartition tp,
        VerificationGuard verificationGuard,
        MemoryRecords batch
    ) {
        if (batch.sizeInBytes() > config(tp).maxMessageSize())
            throw new RecordTooLargeException("Batch is larger than the max message size");

        // We don't want the coordinator to write empty batches.
        if (batch.validBytes() <= 0)
            throw new KafkaException("Coordinator tried to write an empty batch");

        if (writeCount.incrementAndGet() > maxWrites)
            throw new KafkaException("Maximum number of writes reached");

        if (failEndMarker && batch.firstBatch().isControlBatch())
            throw new KafkaException("Couldn't write end marker.");

        time.sleep(10);
        return super.append(tp, verificationGuard, batch);
    }
}
