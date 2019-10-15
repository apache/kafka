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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Implementation of OffsetBackingStore that doesn't actually persist any data. To ensure this
 * behaves similarly to a real backing store, operations are executed asynchronously on a
 * background thread.
 */
public class MemoryOffsetBackingStore implements OffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(MemoryOffsetBackingStore.class);

    protected Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
    protected ExecutorService executor;

    public MemoryOffsetBackingStore() {

    }

    @Override
    public void configure(WorkerConfig config) {
    }

    @Override
    public void start() {
        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop MemoryOffsetBackingStore. Exiting without cleanly " +
                        "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    @Override
    public OffsetReadFuture get(final Collection<ByteBuffer> keys) {
        return new MemoryOffsetReadFuture(keys);
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final Callback<Void> callback) {
        return executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                    data.put(entry.getKey(), entry.getValue());
                }
                save();
                if (callback != null)
                    callback.onCompletion(null, null);
                return null;
            }
        });
    }

    // Hook to allow subclasses to persist data
    protected void save() {

    }

    private class MemoryOffsetReadFuture implements OffsetReadFuture {
        private final Map<ByteBuffer, ByteBuffer> result;
        private final Collection<ByteBuffer> keys;
        private final CountDownLatch completed;
        private final Future<?> underlyingOffsetFuture;

        public MemoryOffsetReadFuture(Collection<ByteBuffer> keys) {
            this.result = new HashMap<>();
            this.keys = keys;
            this.completed = new CountDownLatch(1);
            this.underlyingOffsetFuture = executor.submit(new Runnable() {
                @Override
                public void run() {
                    synchronized (this) {
                        if (isDone()) {
                            return;
                        }
                        collectResults();
                        completed.countDown();
                    }
                }
            });
        }

        @Override
        public void prematurelyComplete() {
            synchronized (this) {
                if (isDone()) {
                    return;
                }
                collectResults();
                completed.countDown();
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return underlyingOffsetFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return underlyingOffsetFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return completed.getCount() == 0;
        }

        @Override
        public Map<ByteBuffer, ByteBuffer> get() throws InterruptedException {
            completed.await();
            return result;
        }

        @Override
        public Map<ByteBuffer, ByteBuffer> get(long timeout, TimeUnit unit)
            throws TimeoutException, InterruptedException {
            if (!completed.await(timeout, unit))
                throw new TimeoutException("Timed out waiting for future");
            return result;
        }

        private void collectResults() {
            for (ByteBuffer key : keys) {
                result.put(key, data.get(key));
            }
        }
    }
}
