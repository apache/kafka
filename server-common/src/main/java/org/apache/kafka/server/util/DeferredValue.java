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
package org.apache.kafka.server.util;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * A value that may not be available at the time of construction, but can be available at some point in the future.
 * This object is a simple wrapper around CompletableFuture.
 *
 * It provides some convenience methods for getting the value without blocking, and for blocking until the value is available.
 */
public class DeferredValue<T> {
    private final CompletableFuture<T> future;
    private final T defaultValue;

    private DeferredValue(CompletableFuture<T> future, T defaultValue) {
        this.future = future;
        this.defaultValue = defaultValue;
    }

    public static <T> DeferredValue<T> completed(T value) {
        return new DeferredValue<>(CompletableFuture.completedFuture(value), value);
    }

    public static <T> DeferredValue<T> incomplete(T defaultValue) {
        return new DeferredValue<>(new CompletableFuture<>(), defaultValue);
    }

    public void complete(T value) {
        future.complete(value);
    }

    /**
     * Get the value if it is available, or return the default value if it is not available.
     */
    public T getNow() {
        return future.getNow(defaultValue);
    }

    /**
     * Get the value if it is available.
     *
     * @throws IllegalStateException if the value is not available yet
     * @throws ExecutionException    if the value completed exceptionally
     * @throws InterruptedException  if the current thread was interrupted
     */
    public T getOrThrow() throws ExecutionException, InterruptedException {
        if (isDone()) {
            return future.get();
        } else {
            throw new IllegalStateException("Value is not available yet");
        }
    }

    /**
     * Wait for the value to be available, with logging.
     */
    public T waitWithLogging(
        Logger log,
        String prefix,
        String action,
        Deadline deadline,
        Time time
    ) throws Throwable {
        return FutureUtils.waitWithLogging(log, prefix, action, future, deadline, time);
    }

    public boolean isDone() {
        return future.isDone();
    }
}
