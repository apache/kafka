/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;


/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {
    public int recordCount = 0;
    public final long created;
    public final MemoryRecords records;
    public final TopicPartition topicPartition;
    private final ProduceRequestResult produceFuture;
    private final List<Thunk> thunks;

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.created = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(byte[] key, byte[] value, CompressionType compression, Callback callback) {
        if (!this.records.hasRoomFor(key, value)) {
            return null;
        } else {
            this.records.append(0L, key, value, compression);
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount);
            if (callback != null)
                thunks.add(new Thunk(callback, this.recordCount));
            this.recordCount++;
            return future;
        }
    }

    /**
     * Complete the request
     * 
     * @param offset The offset
     * @param errorCode The error code or 0 if no error
     */
    public void done(long offset, RuntimeException exception) {
        this.produceFuture.done(topicPartition, offset, exception);
        // execute callbacks
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                Thunk thunk = this.thunks.get(i);
                if (exception == null)
                    thunk.callback.onCompletion(new RecordMetadata(topicPartition, this.produceFuture.baseOffset() + thunk.relativeOffset),
                                                null);
                else
                    thunk.callback.onCompletion(null, exception);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * A callback and the associated RecordSend argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final long relativeOffset;

        public Thunk(Callback callback, long relativeOffset) {
            this.callback = callback;
            this.relativeOffset = relativeOffset;
        }
    }
}