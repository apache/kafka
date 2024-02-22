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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@code ShareFetchBuffer} buffers up {@link CompletedShareFetch the results} from the broker responses
 * as they are received. It is essentially a wrapper around a {@link java.util.Queue} of {@link CompletedShareFetch}.
 * There is at most once {@link CompletedShareFetch} per partition in the queue.
 *
 * <p><em>Note</em>: this class is thread-safe with the intention that {@link CompletedShareFetch the data} will be
 * "produced" by a background thread and consumed by the application thread.
 */
public class ShareFetchBuffer implements AutoCloseable {

    private final Logger log;
    private final ConcurrentLinkedQueue<CompletedShareFetch> completedFetches;
    private final Lock lock;
    private final Condition notEmptyCondition;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();
    private final AtomicBoolean wokenUp = new AtomicBoolean(false);
    private CompletedShareFetch nextInLineFetch;

    public ShareFetchBuffer(final LogContext logContext) {
        this.log = logContext.logger(ShareFetchBuffer.class);
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.lock = new ReentrantLock();
        this.notEmptyCondition = lock.newCondition();
    }

    void add(CompletedShareFetch completedFetch) {
        lock.lock();
        try {
            completedFetches.add(completedFetch);
            notEmptyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }

    CompletedShareFetch nextInLineFetch() {
        lock.lock();
        try {
            return nextInLineFetch;
        } finally {
            lock.unlock();
        }
    }

    void setNextInLineFetch(CompletedShareFetch nextInLineFetch) {
        lock.lock();
        try {
            this.nextInLineFetch = nextInLineFetch;
        } finally {
            lock.unlock();
        }
    }

    CompletedShareFetch peek() {
        lock.lock();
        try {
            return completedFetches.peek();
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    CompletedShareFetch poll() {
        lock.lock();
        try {
            return completedFetches.poll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Allows the caller to await presence of data in the buffer. The method will block, returning only
     * under one of the following conditions:
     *
     * <ol>
     *     <li>The buffer was already non-empty on entry</li>
     *     <li>The buffer was populated during the wait</li>
     *     <li>The remaining time on the {@link Timer timer} elapsed</li>
     *     <li>The thread was interrupted</li>
     * </ol>
     *
     * @param timer Timer that provides time to wait
     */
    void awaitNotEmpty(Timer timer) {
        lock.lock();
        try {

            while (completedFetches.isEmpty() && !wokenUp.compareAndSet(true, false)) {
                // Update the timer before we head into the loop in case it took a while to get the lock.
                timer.update();

                if (timer.isExpired())
                    break;

                if (!notEmptyCondition.await(timer.remainingMs(), TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Timeout waiting for results from fetching records", e);
        } finally {
            lock.unlock();
            timer.update();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            idempotentCloser.close(
                    () -> { },
                    () -> log.warn("The fetch buffer was already closed")
            );
        } finally {
            lock.unlock();
        }
    }
}