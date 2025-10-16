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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A class that models the future completion of a produce request for a single partition. There is one of these per
 * partition in a produce request and it is shared by all the {@link RecordMetadata} instances that are batched together
 * for the same partition in the request.
 */
public class ProduceRequestResult {

    private final CountDownLatch latch = new CountDownLatch(1);
    private final TopicPartition topicPartition;

    /**
     * List of dependent ProduceRequestResults created when this batch is split.
     * When a batch is too large to send, it's split into multiple smaller batches.
     * The original batch's ProduceRequestResult tracks all the split batches here
     * so that flush() can wait for all splits to complete via awaitAllDependents().
     */
    private final List<ProduceRequestResult> dependentResults = new ArrayList<>();

    private volatile Long baseOffset = null;
    private volatile long logAppendTime = RecordBatch.NO_TIMESTAMP;
    private volatile Function<Integer, RuntimeException> errorsByIndex;

    /**
     * Create an instance of this class.
     *
     * @param topicPartition The topic and partition to which this record set was sent
     */
    public ProduceRequestResult(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    /**
     * Set the result of the produce request.
     *
     * @param baseOffset The base offset assigned to the record
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param errorsByIndex Function mapping the batch index to the exception, or null if the response was successful
     */
    public void set(long baseOffset, long logAppendTime, Function<Integer, RuntimeException> errorsByIndex) {
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.errorsByIndex = errorsByIndex;
    }

    /**
     * Mark this request as complete and unblock any threads waiting on its completion.
     */
    public void done() {
        if (baseOffset == null)
            throw new IllegalStateException("The method `set` must be invoked before this method.");
        this.latch.countDown();
    }

    /**
     * Add a dependent ProduceRequestResult.
     * This is used when a batch is split into multiple batches - in some cases like flush(), the original
     * batch's result should not complete until all split batches have completed.
     *
     * @param dependentResult The dependent result to wait for
     */
    public void addDependent(ProduceRequestResult dependentResult) {
        synchronized (dependentResults) {
            dependentResults.add(dependentResult);
        }
    }

    /**
     * Await the completion of this request.
     *
     * This only waits for THIS request's latch and not dependent results.
     * When a batch is split into multiple batches, dependent results are created and tracked
     * separately, but this method does not wait for them. Individual record futures automatically
     * handle waiting for their respective split batch via {@link FutureRecordMetadata#chain(FutureRecordMetadata)},
     * which redirects the future to point to the correct split batch's result.
     *
     * For flush() semantics that require waiting for all dependent results, use
     * {@link #awaitAllDependents()}.
     */
    public void await() throws InterruptedException {
        latch.await();
    }

    /**
     * Await the completion of this request (up to the given time interval)
     * @param timeout The maximum time to wait
     * @param unit The unit for the max time
     * @return true if the request completed, false if we timed out
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        return latch.await(timeout, unit);
    }

    /**
     * Await the completion of this request and all the dependent requests.
     *
     * This method is used by flush() to ensure all split batches have completed before
     * returning. This method waits for all dependent {@link ProduceRequestResult}s that
     * were created when the batch was split.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void awaitAllDependents() throws InterruptedException {
        Queue<ProduceRequestResult> toWait = new ArrayDeque<>();
        toWait.add(this);

        while (!toWait.isEmpty()) {
            ProduceRequestResult current = toWait.poll();

            // first wait for THIS result's latch to be released
            current.latch.await();

            // add all dependent split batches to the queue.
            // we synchronize to get a consistent snapshot, then release the lock
            // before continuing but the actual waiting happens outside the lock.
            synchronized (current.dependentResults) {
                toWait.addAll(current.dependentResults);
            }
        }
    }

    /**
     * The base offset for the request (the first offset in the record set)
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * Return true if log append time is being used for this topic
     */
    public boolean hasLogAppendTime() {
        return logAppendTime != RecordBatch.NO_TIMESTAMP;
    }

    /**
     * The log append time or -1 if CreateTime is being used
     */
    public long logAppendTime() {
        return logAppendTime;
    }

    /**
     * The error thrown (generally on the server) while processing this request
     */
    public RuntimeException error(int batchIndex) {
        if (errorsByIndex == null) {
            return null;
        } else {
            return errorsByIndex.apply(batchIndex);
        }
    }

    /**
     * The topic and partition to which the record was appended
     */
    public TopicPartition topicPartition() {
        return topicPartition;
    }

    /**
     * Has the request completed?
     *
     * This method only checks if THIS request has completed and not its dependent results.
     * When a batch is split into multiple batches, the dependent split batches are tracked
     * separately. Individual record futures handle waiting for their respective split
     * batch via {@link FutureRecordMetadata#chain(FutureRecordMetadata)}, which updates the
     * {@code nextRecordMetadata} pointer to follow the correct split batch.
     *
     * For flush() semantics that require waiting for all dependent results, use
     * {@link #awaitAllDependents()}.
     */
    public boolean completed() {
        return this.latch.getCount() == 0L;
    }
}
