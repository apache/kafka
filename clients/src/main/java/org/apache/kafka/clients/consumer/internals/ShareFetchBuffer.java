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

import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@code ShareFetchBuffer} buffers up {@link CompletedFetch the results} from the broker responses
 * as they are received. It is essentially a wrapper around a {@link java.util.Queue} of {@link CompletedFetch}.
 * There is at most once {@link CompletedFetch} per partition in the queue.
 *
 * <p/>
 *
 * <em>Note</em>: this class is thread-safe with the intention that {@link CompletedFetch the data} will be
 * "produced" by a background thread and consumed by the application thread.
 */
public class ShareFetchBuffer implements AutoCloseable {

    private final Logger log;
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final Lock lock;
    private final IdempotentCloser idempotentCloser = new IdempotentCloser();

    public ShareFetchBuffer(final LogContext logContext) {
        this.log = logContext.logger(ShareFetchBuffer.class);
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.lock = new ReentrantLock();
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

    public void add(CompletedFetch completedFetch) {
        lock.lock();
        try {
            completedFetches.add(completedFetch);
        } finally {
            lock.unlock();
        }
    }
}