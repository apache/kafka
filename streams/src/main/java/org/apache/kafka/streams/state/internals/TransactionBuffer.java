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
package org.apache.kafka.streams.state.internals;

import java.io.Closeable;
import java.util.Optional;

/**
 * A transaction buffer that accumulates writes and flushes them atomically.
 * <p>
 * Staged writes are visible to all readers via {@link #get(Comparable)} and scan methods,
 * but are only applied to the underlying store on {@link #commit()}.
 *
 * @param <K> the key type, must be {@link Comparable}
 */
interface TransactionBuffer<K extends Comparable<K>> extends Closeable {

    /**
     * Stage a put or delete. A null value represents a tombstone (pending delete).
     * Must be called from the owner thread only.
     */
    void stage(K key, byte[] value);

    /**
     * Look up a key in the staging buffer.
     * <p>
     * Returns {@code null} if the key has no staged entry (caller should fall back to the
     * base store). Returns {@link Optional#empty()} if the key has a staged tombstone
     * (pending delete). Returns {@code Optional.of(value)} if the key has a staged value.
     * <p>
     * Can be called from any thread without locking.
     */
    Optional<byte[]> get(K key);

    /**
     * Return a scan iterator over all entries in the given direction (staged overlay
     * merged with base store). Safe to call from any thread. Owner-thread calls use a
     * lock-free fast path; other threads acquire a read lock to snapshot staged writes.
     */
    ManagedKeyValueIterator<K, byte[]> all(boolean forward);

    /**
     * Return a range scan iterator with configurable direction and upper bound inclusiveness
     * (staged overlay merged with base store). Safe to call from any thread. Owner-thread
     * calls use a lock-free fast path; other threads acquire a read lock to snapshot staged
     * writes.
     */
    ManagedKeyValueIterator<K, byte[]> range(K from, K to, boolean forward, boolean toInclusive);

    /**
     * Atomically apply all staged writes to the underlying store and clear the staging area.
     * Must be called from the owner thread only.
     */
    void commit();

    /**
     * Discard all staged writes without applying them.
     * Must be called from the owner thread only.
     */
    void rollback();

    /**
     * Returns true if there are no staged writes.
     */
    boolean isEmpty();

    /**
     * Returns an approximation of the number of uncommitted bytes currently staged in this buffer.
     * This is not exact — it does not account for overwrites of the same key, and may not include
     * all overhead (e.g. object headers in the staging map). It is intended for use in triggering
     * early commits when the buffer grows too large.
     */
    long approximateNumUncommittedBytes();

    /**
     * Release any resources held by this buffer.
     */
    @Override
    void close();
}
