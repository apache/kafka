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

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * {@code ThreadSafeReference} serves as a thread-safe primitive around an object reference that provides
 * utility methods for more ergonomic use.
 */
public class ThreadSafeReference<T> {

    private final AtomicReference<T> reference = new AtomicReference<>();

    /**
     * Thin wrapper around {@link AtomicReference#get()} that provides a null-safe API via {@link Optional}.
     */
    public Optional<T> get() {
        return Optional.ofNullable(reference.get());
    }

    /**
     * If the underlying reference is nonnull, the given {@link Consumer action} is invoked.
     */
    public void ifPresent(Consumer<T> action) {
        get().ifPresent(action);
    }

    /**
     * Thin wrapper around {@link AtomicReference#getAndSet(Object)} that provides a null-safe API via {@link Optional}.
     */
    public Optional<T> getAndClear() {
        return Optional.ofNullable(reference.getAndSet(null));
    }

    /**
     * Wrapper around {@link #getAndClear()} and {@link Optional#ifPresent(Consumer)} that retrieves and clears out
     * the underlying reference in a single, atomic operation, and then invokes the given {@link Consumer} if the
     * value was present. Lastly, it returns the present/empty flag so that the caller can short-circuit with less
     * boilerplate.
     */
    public boolean getClearAndRun(Consumer<T> action) {
        Optional<T> value = getAndClear();

        if (value.isPresent()) {
            action.accept(value.get());
            return true;
        } else {
            return false;
        }
    }

    public void set(T value) {
        reference.set(value);
    }
}
