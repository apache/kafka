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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability.Evolving;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;

import java.util.Map;

/**
 * A storage engine for managing state maintained by a stream processor.
 * <p>
 * If the store is implemented as a persistent store, it <em>must</em> use the store name as directory name and write
 * all data into this store directory.
 * The store directory must be created with the state directory.
 * The state directory can be obtained via {@link ProcessorContext#stateDir() #stateDir()} using the
 * {@link ProcessorContext} provided via {@link #init(StateStoreContext, StateStore) init(...)}.
 * <p>
 * Using nested store directories within the state directory isolates different state stores.
 * If a state store would write into the state directory directly, it might conflict with others state stores and thus,
 * data might get corrupted and/or Streams might fail with an error.
 * Furthermore, Kafka Streams relies on using the store name as store directory name to perform internal cleanup tasks.
 * <p>
 * This interface does not specify any query capabilities, which, of course,
 * would be query engine specific. Instead, it just specifies the minimum
 * functionality required to reload a storage engine from its changelog as well
 * as basic lifecycle management.
 */
public interface StateStore {

    /**
     * The name of this store.
     * @return the storage name
     */
    String name();

    /**
     * Initializes this state store.
     * <p>
     * The implementation of this function must register the root store in the context via the
     * {@link org.apache.kafka.streams.processor.ProcessorContext#register(StateStore, StateRestoreCallback)} function,
     * where the first {@link StateStore} parameter should always be the passed-in {@code root} object, and
     * the second parameter should be an object of user's implementation
     * of the {@link StateRestoreCallback} interface used for restoring the state store from the changelog.
     * <p>
     * Note that if the state store engine itself supports bulk writes, users can implement another
     * interface {@link BatchingStateRestoreCallback} which extends {@link StateRestoreCallback} to
     * let users implement bulk-load restoration logic instead of restoring one record at a time.
     * <p>
     * This method is not called if {@link StateStore#init(StateStoreContext, StateStore)}
     * is implemented.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     * @deprecated Since 2.7.0. Callers should invoke {@link #init(StateStoreContext, StateStore)} instead.
     *             Implementers may choose to implement this method for backward compatibility or to throw an
     *             informative exception instead.
     */
    @Deprecated
    void init(org.apache.kafka.streams.processor.ProcessorContext context, StateStore root);

    /**
     * Initializes this state store.
     * <p>
     * The implementation of this function must register the root store in the context via the
     * {@link StateStoreContext#register(StateStore, StateRestoreCallback, CommitCallback)} function, where the
     * first {@link StateStore} parameter should always be the passed-in {@code root} object, and
     * the second parameter should be an object of user's implementation
     * of the {@link StateRestoreCallback} interface used for restoring the state store from the changelog.
     * <p>
     * Note that if the state store engine itself supports bulk writes, users can implement another
     * interface {@link BatchingStateRestoreCallback} which extends {@link StateRestoreCallback} to
     * let users implement bulk-load restoration logic instead of restoring one record at a time.
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    default void init(final StateStoreContext context, final StateStore root) {
        init(StoreToProcessorContextAdapter.adapt(context), root);
    }

    /**
     * Flush any cached data
     *
     * @deprecated Use {@link org.apache.kafka.streams.processor.api.ProcessingContext#commit() ProcessorContext#commit()}
     *             to request a Task commit instead.
     */
    @Deprecated
    default void flush() {
        // no-op
    }

    /**
     * Commit all written records to this StateStore.
     * <p>
     * This method MUST NOT be called by users from {@link org.apache.kafka.streams.processor.api.Processor processors},
     * as doing so may violate the consistency guarantees provided by this store, and expected by Kafka Streams.
     * Instead, users should call {@link org.apache.kafka.streams.processor.api.ProcessingContext#commit()
     * ProcessorContext#commit()} to request a Task commit.
     * <p>
     * When called, every write written since the last call to {@link #commit(Map)}, or since this store was {@link
     * #init(StateStoreContext, StateStore) opened} will be made available to readers using the {@link
     * org.apache.kafka.common.IsolationLevel#READ_COMMITTED READ_COMMITTED} {@link
     * org.apache.kafka.common.IsolationLevel IsolationLevel}.
     * <p>
     * If {@link #persistent()} returns {@code true}, after this method returns, all records written since the last call
     * to {@link #commit(Map)} are guaranteed to be persisted to disk, and available to read, even if this {@link
     * StateStore} is {@link #close() closed} and subsequently {@link #init(StateStoreContext, StateStore) re-opened}.
     * <p>
     * If {@link #managesOffsets()} <em>also</em> returns {@code true}, the given {@code changelogOffsets} will be
     * guaranteed to be persisted to disk along with the written records.
     * <p>
     * {@code changelogOffsets} will usually contain a single partition, in the case of a regular StateStore. However,
     * they may contain multiple partitions in the case of a Global StateStore with multiple partitions. All provided
     * partitions <em>MUST</em> be persisted to disk.
     * <p>
     * Implementations <em>SHOULD</em> ensure that {@code changelogOffsets} are comitted to disk atomically with the
     * records they represent.
     *
     * @param changelogOffsets The changelog offset(s) corresponding to the most recently written records.
     */
    default void commit(final Map<TopicPartition, Long> changelogOffsets) {
        flush();
    }

    /**
     * Returns the most recently {@link #commit(Map) committed} offset for the given {@link TopicPartition}.
     * <p>
     * If {@link #managesOffsets()} and {@link #persistent()} both return {@code true}, this method will return the
     * offset that corresponds to the changelog record most recently written to this store, for the given {@code
     * partition}.
     * <p>
     * This method provides readers using the {@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED} {@link
     * org.apache.kafka.common.IsolationLevel} a means to determine the point in the changelog that this StateStore
     * currently represents.
     *
     * @param partition The partition to get the committed offset for.
     * @return The last {@link #commit(Map) committed} offset for the {@code partition}; or {@code null} if no offset
     *         has been committed for the partition, or if either {@link #persistent()} or {@link #managesOffsets()}
     *         return {@code false}.
     */
    default Long getCommittedOffset(final TopicPartition partition) {
        return null;
    }

    /**
     * Determines if this StateStore manages its own offsets.
     * <p>
     * If this method returns {@code true}, then offsets provided to {@link #commit(Map)} will be retrievable using
     * {@link #getCommittedOffset(TopicPartition)}, even if the store is {@link #close() closed} and later re-opened.
     * <p>
     * If this method returns {@code false}, offsets provided to {@link #commit(Map)} will be ignored, and {@link
     * #getCommittedOffset(TopicPartition)} will be expected to always return {@code null}.
     * <p>
     * This method is provided to enable custom StateStores to opt-in to managing their own offsets. This is highly
     * recommended, if possible, to ensure that custom StateStores provide the consistency guarantees that Kafka Streams
     * expects when operating under the {@code exactly-once} {@code processing.mode}.
     *
     * @return Whether this StateStore manages its own offsets.
     */
    default boolean managesOffsets() {
        return false;
    }

    /**
     * Close the storage engine.
     * Note that this function needs to be idempotent since it may be called
     * several times on the same state store.
     * <p>
     * Users only need to implement this function but should NEVER need to call this api explicitly
     * as it will be called by the library automatically when necessary
     */
    void close();

    /**
     * Return if the storage is persistent or not.
     *
     * @return  {@code true} if the storage is persistent&mdash;{@code false} otherwise
     */
    boolean persistent();

    /**
     * Is this store open for reading and writing
     * @return {@code true} if the store is open
     */
    boolean isOpen();

    /**
     * Execute a query. Returns a QueryResult containing either result data or
     * a failure.
     * <p>
     * If the store doesn't know how to handle the given query, the result
     * shall be a {@link FailureReason#UNKNOWN_QUERY_TYPE}.
     * If the store couldn't satisfy the given position bound, the result
     * shall be a {@link FailureReason#NOT_UP_TO_BOUND}.
     * <p>
     * Note to store implementers: if your store does not support position tracking,
     * you can correctly respond {@link FailureReason#NOT_UP_TO_BOUND} if the argument is
     * anything but {@link PositionBound#unbounded()}. Be sure to explain in the failure message
     * that bounded positions are not supported.
     * <p>
     * @param query The query to execute
     * @param positionBound The position the store must be at or past
     * @param config Per query configuration parameters, such as whether the store should collect detailed execution
     * info for the query
     * @param <R> The result type
     */
    @Evolving
    default <R> QueryResult<R> query(
        final Query<R> query,
        final PositionBound positionBound,
        final QueryConfig config) {
        // If a store doesn't implement a query handler, then all queries are unknown.
        return QueryResult.forUnknownQueryType(query, this);
    }

    /**
     * Returns the position the state store is at with respect to the input topic/partitions
     */
    @Evolving
    default Position getPosition() {
        throw new UnsupportedOperationException(
            "getPosition is not implemented by this StateStore (" + getClass() + ")"
        );
    }

    /**
     * Return an approximate count of memory used by records not yet committed to this StateStore.
     * <p>
     * This method will return an approximation of the memory that would be freed by the next call to {@link
     * #commit(Map)}.
     * <p>
     * If no records have been written to this store since {@link #init(StateStoreContext, StateStore) opening}, or
     * since the last {@link #commit(Map)}; or if this store does not support atomic transactions, it will return {@code
     * 0}, as no records are currently being buffered.
     *
     * @return The approximate size of all records awaiting {@link #commit(Map)}; or {@code 0} if this store does not
     *         support transactions, or has not been written to since {@link #init(StateStoreContext, StateStore)} or
     *         last {@link #commit(Map)}.
     */
    default long approximateNumUncommittedBytes() {
        return 0;
    }
}
