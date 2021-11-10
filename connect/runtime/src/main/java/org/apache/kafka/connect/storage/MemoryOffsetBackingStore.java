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
import org.apache.kafka.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
        executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(
                this.getClass().getSimpleName() + "-%d", false));
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
    public Future<Map<ByteBuffer, ByteBuffer>> get(final Collection<ByteBuffer> keys) {
        return executor.submit(() -> {
            Map<ByteBuffer, ByteBuffer> result = new HashMap<>();
            for (ByteBuffer key : keys) {
                result.put(key, data.get(key));
            }
            return result;
        });
    }

    @Override
    public Future<Void> set(final Map<ByteBuffer, ByteBuffer> values,
                            final Callback<Void> callback) {
        return executor.submit(() -> {
            for (Map.Entry<ByteBuffer, ByteBuffer> entry : values.entrySet()) {
                data.put(entry.getKey(), entry.getValue());
            }
            save();
            if (callback != null)
                callback.onCompletion(null, null);
            return null;
        });
    }

    // Hook to allow subclasses to persist data
    protected void save() {

    }
}
