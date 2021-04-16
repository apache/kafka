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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The future result of a record send
 */
public final class FutureRecordMetadata implements Future<RecordMetadata> {

    private final ProduceRequestResult result;
    private final int batchIndex;
    private final long createTimestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final Time time;
    private volatile FutureRecordMetadata nextRecordMetadata = null;

    public FutureRecordMetadata(ProduceRequestResult result, int batchIndex, long createTimestamp, int serializedKeySize,
                                int serializedValueSize, Time time) {
        this.result = result;
        this.batchIndex = batchIndex;
        this.createTimestamp = createTimestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.time = time;
    }

    @Override
    public boolean cancel(boolean interrupt) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        this.result.await();
        if (nextRecordMetadata != null)
            return nextRecordMetadata.get();
        return valueOrError();
    }

    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        // Handle overflow.
        long now = time.milliseconds();
        long timeoutMillis = unit.toMillis(timeout);
        long deadline = Long.MAX_VALUE - timeoutMillis < now ? Long.MAX_VALUE : now + timeoutMillis;
        boolean occurred = this.result.await(timeout, unit);
        if (!occurred)
            throw new TimeoutException("Timeout after waiting for " + timeoutMillis + " ms.");
        if (nextRecordMetadata != null)
            return nextRecordMetadata.get(deadline - time.milliseconds(), TimeUnit.MILLISECONDS);
        return valueOrError();
    }

    /**
     * This method is used when we have to split a large batch in smaller ones. A chained metadata will allow the
     * future that has already returned to the users to wait on the newly created split batches even after the
     * old big batch has been deemed as done.
     */
    void chain(FutureRecordMetadata futureRecordMetadata) {
        if (nextRecordMetadata == null)
            nextRecordMetadata = futureRecordMetadata;
        else
            nextRecordMetadata.chain(futureRecordMetadata);
    }

    RecordMetadata valueOrError() throws ExecutionException {
        RuntimeException exception = this.result.error(batchIndex);
        if (exception != null)
            throw new ExecutionException(exception);
        else
            return value();
    }

    RecordMetadata value() {
        if (nextRecordMetadata != null)
            return nextRecordMetadata.value();
        return new RecordMetadata(result.topicPartition(), this.result.baseOffset(), this.batchIndex,
                                  timestamp(), this.serializedKeySize, this.serializedValueSize);
    }

    private long timestamp() {
        return result.hasLogAppendTime() ? result.logAppendTime() : createTimestamp;
    }

    @Override
    public boolean isDone() {
        if (nextRecordMetadata != null)
            return nextRecordMetadata.isDone();
        return this.result.completed();
    }

}
