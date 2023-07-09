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
package org.apache.kafka.streams.state;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStore;

/**
 * A key-value store that stores multiple record versions per key, and supports timestamp-based
 * retrieval operations to return the latest record (per key) as of a specified timestamp.
 * Only one record is stored per key and timestamp, i.e., a second call to
 * {@link #put(Object, Object, long)} with the same key and timestamp will replace the first.
 * <p>
 * Each store instance has an associated, fixed-duration "history retention" which specifies
 * how long old record versions should be kept for. In particular, a versioned store guarantees
 * to return accurate results for calls to {@link #get(Object, long)} where the provided timestamp
 * bound is within history retention of the current observed stream time. (Queries with timestamp
 * bound older than the specified history retention are considered invalid.)
 * <p>
 * The store's "history retention" also doubles as its "grace period," which determines how far
 * back in time writes to the store will be accepted. A versioned store will not accept writes
 * (inserts, updates, or deletions) if the timestamp associated with the write is older than the
 * current observed stream time by more than the grace period.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface VersionedKeyValueStore<K, V> extends StateStore {

    long PUT_RETURN_CODE_VALID_TO_UNDEFINED = -1L;
    long PUT_RETURN_CODE_NOT_PUT = Long.MIN_VALUE;

    /**
     * Add a new record version associated with the specified key and timestamp.
     * <p>
     * If the timestamp associated with the new record version is older than the store's
     * grace period (i.e., history retention) relative to the current observed stream time,
     * then the record will not be added.
     *
     * @param key       The key
     * @param value     The value, it can be {@code null}. {@code null} is interpreted as a delete.
     * @param timestamp The timestamp for this record version
     * @return The validTo timestamp of the newly put record. Two special values, {@code -1} and
     *         {@code Long.MIN_VALUE} carry specific meanings. {@code -1} indicates that the
     *         record that was put is the latest record version for its key, and therefore the
     *         validTo timestamp is undefined. {@code Long.MIN_VALUE} indicates that the record
     *         was not put, due to grace period having been exceeded.
     * @throws NullPointerException If {@code null} is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    long put(K key, V value, long timestamp);

    /**
     * Delete the value associated with this key from the store, at the specified timestamp
     * (if there is such a value), and return the deleted value.
     * <p>
     * If the timestamp associated with this deletion is older than the store's grace period
     * (i.e., history retention) relative to the current observed stream time, then the deletion
     * will not be performed and {@code null} will be returned.
     * <p>
     * As a consequence of the above, the way to delete a record version is <it>not</it>
     * to first call {@link #get(Object) #get(key)} or {@link #get(Object, long) #get(key, timestamp)}
     * and use the returned {@link VersionedRecord#timestamp()} in a call to this
     * {@code delete(key, timestamp)} method, as the returned timestamp may be older than
     * the store's grace period (i.e., history retention) and will therefore not take place.
     * Instead, you should pass a business logic inferred timestamp that specifies when
     * the delete actually happens. For example, it could be the timestamp of the currently
     * processed input record or the current stream time.
     * <p>
     * This operation is semantically equivalent to {@link #get(Object, long) #get(key, timestamp)}
     * followed by {@link #put(Object, Object, long) #put(key, null, timestamp)}, with
     * a caveat that if the deletion timestamp is older than the store's grace period
     * (i.e., history retention) then the return value is always {@code null}, regardless
     * of what {@link #get(Object, long) #get(key, timestamp)} would return.
     *
     * @param key       The key
     * @param timestamp The timestamp for this delete
     * @return The value and timestamp of the record associated with this key as of
     *         the deletion timestamp (inclusive), or {@code null} if no such record exists
     *         (including if the deletion timestamp is older than this store's history
     *         retention time, i.e., the store no longer contains data for the provided
     *         timestamp). Note that the record timestamp {@code r.timestamp()} of the
     *         returned {@link VersionedRecord} may be smaller than the provided deletion
     *         timestamp.
     * @throws NullPointerException If {@code null} is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    VersionedRecord<V> delete(K key, long timestamp);

    /**
     * Get the current (i.e., latest by timestamp) record associated with this key.
     *
     * @param key The key to fetch
     * @return The value and timestamp of the current record associated with this key, or
     *         {@code null} if there is no current record for this key.
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    VersionedRecord<V> get(K key);

    /**
     * Get the record associated with this key as of the specified timestamp (i.e.,
     * the existing record with the largest timestamp not exceeding the provided
     * timestamp bound).
     *
     * @param key           The key to fetch
     * @param asOfTimestamp The timestamp bound. This bound is inclusive; if a record
     *                      (for the specified key) exists with this timestamp, then
     *                      this is the record that will be returned.
     * @return The value and timestamp of the record associated with this key
     *         as of the provided timestamp, or {@code null} if no such record exists
     *         (including if the provided timestamp bound is older than this store's history
     *         retention time, i.e., the store no longer contains data for the provided
     *         timestamp). Note that the record timestamp {@code r.timestamp()} of the
     *         returned {@link VersionedRecord} may be smaller than the provided timestamp
     *         bound. Additionally, if the latest record version for the key is eligible
     *         for the provided timestamp bound, then that record will be returned even if
     *         the timestamp bound is older than the store's history retention.
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    VersionedRecord<V> get(K key, long asOfTimestamp);
}