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

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A utility class providing helper methods for working with {@link Lock} objects.
 * This class simplifies the usage of locks by encapsulating common patterns,
 * such as acquiring and releasing locks in a safe manner.
 */
public class LockUtils {
    @FunctionalInterface
    public interface ThrowingSupplier<T, E extends Exception> {
        T get() throws E;
    }
    @FunctionalInterface
    public interface ThrowingRunnable<E extends Exception> {
        void run() throws E;
    }

    /**
     * Executes the given {@link ThrowingSupplier} within the context of the specified {@link Lock}.
     * The lock is acquired before executing the supplier and released after the execution,
     * ensuring that the lock is always released, even if an exception is thrown.
     *
     * @param <T>      the type of the result returned by the supplier
     * @param <E>      the type of exception that may be thrown by the supplier
     * @param lock     the lock to be acquired and released
     * @param supplier the supplier to be executed within the lock context
     * @return the result of the supplier
     * @throws E if an exception occurs during the execution of the supplier
     * @throws NullPointerException if either {@code lock} or {@code supplier} is null
     */
    public static <T, E extends Exception> T inLock(Lock lock, ThrowingSupplier<T, E> supplier) throws E {
        Objects.requireNonNull(lock, "Lock must not be null");
        Objects.requireNonNull(supplier, "Supplier must not be null");

        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Executes the given {@link ThrowingSupplier} within the context of the read lock of the specified {@link ReadWriteLock}.
     * see {@link LockUtils#inLock(Lock, ThrowingSupplier)}
     */
    public static <T, E extends Exception> T inReadLock(ReadWriteLock lock, ThrowingSupplier<T, E> supplier) throws E {
        return inLock(lock.readLock(), supplier);
    }

    /**
     * Executes the given {@link ThrowingSupplier} within the context of the write lock of the specified {@link ReadWriteLock}.
     * see {@link LockUtils#inLock(Lock, ThrowingSupplier)}
     */
    public static <T, E extends Exception> T inWriteLock(ReadWriteLock lock, ThrowingSupplier<T, E> supplier) throws E {
        return inLock(lock.writeLock(), supplier);
    }

    /**
     * Executes the given {@link ThrowingRunnable} within the context of the specified {@link Lock}.
     * The lock is acquired before executing the runnable and released after the execution,
     * ensuring that the lock is always released, even if an exception is thrown.
     *
     * @param <E>      the type of exception that may be thrown by the runnable
     * @param lock     the lock to be acquired and released
     * @param runnable the runnable to be executed within the lock context
     * @throws E if an exception occurs during the execution of the runnable
     * @throws NullPointerException if either {@code lock} or {@code runnable} is null
     */
    public static <E extends Exception> void inLock(Lock lock, ThrowingRunnable<E> runnable) throws E {
        Objects.requireNonNull(lock, "Lock must not be null");
        Objects.requireNonNull(runnable, "Runnable must not be null");

        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Executes the given {@link ThrowingRunnable} within the context of the read lock of the specified {@link ReadWriteLock}.
     * see {@link LockUtils#inLock(Lock, ThrowingRunnable)}
     */
    public static <E extends Exception> void inReadLock(ReadWriteLock lock, ThrowingRunnable<E> runnable) throws E {
        inLock(lock.readLock(), runnable);
    }

    /**
     * Executes the given {@link ThrowingRunnable} within the context of the write lock of the specified {@link ReadWriteLock}.
     * see {@link LockUtils#inLock(Lock, ThrowingRunnable)}
     */
    public static <E extends Exception> void inWriteLock(ReadWriteLock lock, ThrowingRunnable<E> runnable) throws E {
        inLock(lock.writeLock(), runnable);
    }
}
