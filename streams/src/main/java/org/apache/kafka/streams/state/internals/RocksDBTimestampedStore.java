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

import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatchInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

import static org.apache.kafka.streams.state.TimestampedBytesStore.convertToTimestampedFormat;

/**
 * A persistent key-(value-timestamp) store based on RocksDB.
 */
public class RocksDBTimestampedStore extends RocksDBStore implements TimestampedBytesStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBTimestampedStore.class);

    private static final byte[] TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME = "keyValueWithTimestamp".getBytes(StandardCharsets.UTF_8);

    public RocksDBTimestampedStore(final String name,
                            final String metricsScope) {
        super(name, metricsScope);
    }

    RocksDBTimestampedStore(final String name,
                            final String parentDir,
                            final RocksDBMetricsRecorder metricsRecorder) {
        super(name, parentDir, metricsRecorder);
    }

    @Override
    void openRocksDB(final DBOptions dbOptions,
                     final ColumnFamilyOptions columnFamilyOptions) {
        final List<ColumnFamilyHandle> columnFamilies = openRocksDB(
                dbOptions,
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor(TIMESTAMPED_VALUES_COLUMN_FAMILY_NAME, columnFamilyOptions)
        );
        final ColumnFamilyHandle noTimestampColumnFamily = columnFamilies.get(0);
        final ColumnFamilyHandle withTimestampColumnFamily = columnFamilies.get(1);

        final RocksIterator noTimestampsIter = db.newIterator(noTimestampColumnFamily);
        noTimestampsIter.seekToFirst();
        if (noTimestampsIter.isValid()) {
            log.info("Opening store {} in upgrade mode", name);
            cfAccessor = new DualColumnFamilyAccessor(noTimestampColumnFamily, withTimestampColumnFamily);
        } else {
            log.info("Opening store {} in regular mode", name);
            cfAccessor = new SingleColumnFamilyAccessor(withTimestampColumnFamily);
            noTimestampColumnFamily.close();
        }
        noTimestampsIter.close();
    }

    private class DualColumnFamilyAccessor implements ColumnFamilyAccessor {
        private final ColumnFamilyHandle oldColumnFamily;
        private final ColumnFamilyHandle newColumnFamily;

        private DualColumnFamilyAccessor(final ColumnFamilyHandle oldColumnFamily,
                                         final ColumnFamilyHandle newColumnFamily) {
            this.oldColumnFamily = oldColumnFamily;
            this.newColumnFamily = newColumnFamily;
        }

        @Override
        public void put(final DBAccessor accessor,
                        final byte[] key,
                        final byte[] valueWithTimestamp) {
            synchronized (position) {
                if (valueWithTimestamp == null) {
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try {
                        accessor.delete(newColumnFamily, key);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                } else {
                    try {
                        accessor.delete(oldColumnFamily, key);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while removing key from store " + name, e);
                    }
                    try {
                        accessor.put(newColumnFamily, key, valueWithTimestamp);
                        StoreQueryUtils.updatePosition(position, context);
                    } catch (final RocksDBException e) {
                        // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                        throw new ProcessorStateException("Error while putting key/value into store " + name, e);
                    }
                }
            }
        }

        @Override
        public void prepareBatch(final List<KeyValue<Bytes, byte[]>> entries,
                                 final WriteBatchInterface batch) throws RocksDBException {
            for (final KeyValue<Bytes, byte[]> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                addToBatch(entry.key.get(), entry.value, batch);
            }
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            return get(accessor, key, Optional.empty());
        }

        @Override
        public byte[] get(final DBAccessor accessor, final byte[] key, final ReadOptions readOptions) throws RocksDBException {
            return get(accessor, key, Optional.of(readOptions));
        }

        private byte[] get(final DBAccessor accessor, final byte[] key, final Optional<ReadOptions> readOptions) throws RocksDBException {
            final byte[] valueWithTimestamp = readOptions.isPresent() ? accessor.get(newColumnFamily, readOptions.get(), key) : accessor.get(newColumnFamily, key);
            if (valueWithTimestamp != null) {
                return valueWithTimestamp;
            }

            final byte[] plainValue = readOptions.isPresent() ? accessor.get(oldColumnFamily, readOptions.get(), key) : accessor.get(oldColumnFamily, key);
            if (plainValue != null) {
                final byte[] valueWithUnknownTimestamp = convertToTimestampedFormat(plainValue);
                // this does only work, because the changelog topic contains correct data already
                // for other format changes, we cannot take this short cut and can only migrate data
                // from old to new store on put()
                put(accessor, key, valueWithUnknownTimestamp);
                return valueWithUnknownTimestamp;
            }

            return null;
        }

        @Override
        public byte[] getOnly(final DBAccessor accessor, final byte[] key) throws RocksDBException {
            final byte[] valueWithTimestamp = accessor.get(newColumnFamily, key);
            if (valueWithTimestamp != null) {
                return valueWithTimestamp;
            }

            final byte[] plainValue = accessor.get(oldColumnFamily, key);
            if (plainValue != null) {
                return convertToTimestampedFormat(plainValue);
            }

            return null;
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> range(final DBAccessor accessor,
                                                            final Bytes from,
                                                            final Bytes to,
                                                            final boolean forward) {
            return RocksDBDualCFRangeIterator.of(
                    from,
                    to,
                    accessor.newIterator(oldColumnFamily),
                    accessor.newIterator(newColumnFamily),
                    name,
                    forward,
                    true);
        }

        @Override
        public void deleteRange(final DBAccessor accessor, final byte[] from, final byte[] to) {
            try {
                accessor.deleteRange(oldColumnFamily, from, to);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
            try {
                accessor.deleteRange(newColumnFamily, from, to);
            } catch (final RocksDBException e) {
                // String format is happening in wrapping stores. So formatted message is thrown from wrapping stores.
                throw new ProcessorStateException("Error while removing key from store " + name, e);
            }
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> all(final DBAccessor accessor, final boolean forward) {
            return RocksDBDualCFRangeIterator.of(
                    null,
                    null,
                    accessor.newIterator(oldColumnFamily),
                    accessor.newIterator(newColumnFamily),
                    name,
                    forward,
                    true);
        }

        @Override
        public ManagedKeyValueIterator<Bytes, byte[]> prefixScan(final DBAccessor accessor, final Bytes prefix) {
            final Bytes to = incrementWithoutOverflow(prefix);
            return RocksDBDualCFRangeIterator.of(
                    prefix,
                    to,
                    accessor.newIterator(oldColumnFamily),
                    accessor.newIterator(newColumnFamily),
                    name,
                    true,
                    false
            );
        }

        @Override
        public long approximateNumEntries(final DBAccessor accessor) throws RocksDBException {
            return accessor.approximateNumEntries(oldColumnFamily) +
                    accessor.approximateNumEntries(newColumnFamily);
        }

        @Override
        public void flush(final DBAccessor accessor) throws RocksDBException {
            accessor.flush(oldColumnFamily, newColumnFamily);
        }

        @Override
        public void addToBatch(final byte[] key,
                               final byte[] value,
                               final WriteBatchInterface batch) throws RocksDBException {
            if (value == null) {
                batch.delete(oldColumnFamily, key);
                batch.delete(newColumnFamily, key);
            } else {
                batch.delete(oldColumnFamily, key);
                batch.put(newColumnFamily, key, value);
            }
        }

        @Override
        public void close() {
            oldColumnFamily.close();
            newColumnFamily.close();
        }
    }

    /**
     * A range-based iterator for RocksDB that merges results from two column families.
     *
     * <p>This iterator supports traversal over two RocksDB column families: one containing timestamped values and
     * another containing non-timestamped values. It ensures that the keys from both column families are merged and
     * sorted lexicographically, respecting the iteration order (forward or reverse) and the specified range
     * boundaries.</p>
     *
     * <h2>Key Features</h2>
     *
     * <ul>
     *     <li>Merges results from the "with-timestamp" and "no-timestamp" column families.</li>
     *     <li>Supports range-based queries with open or closed boundaries.</li>
     *     <li>Handles both forward and reverse iteration seamlessly.</li>
     *     <li>Ensures correct handling of inclusive and exclusive upper boundaries.</li>
     *     <li>Integrates efficiently with Kafka Streams state store mechanisms.</li>
     * </ul>
     *
     * <h2>Usage</h2>
     *
     * <p>The iterator can be used for different types of range-based operations, such as:
     * <ul>
     *     <li>Iterating over all keys within a range.</li>
     *     <li>Prefix-based scans (when combined with dynamically calculated range endpoints).</li>
     *     <li>Open-ended range queries (e.g., from a given key to the end of the dataset).</li>
     * </ul>
     * </p>
     *
     * <h2>Implementation Details</h2>
     *
     * <p>The class extends {@link AbstractIterator} and implements {@link ManagedKeyValueIterator}. It uses RocksDB's
     * native iterators for efficient traversal of keys within the specified range. Keys from the two column families
     * are merged during iteration, ensuring proper order and de-duplication where applicable.</p>
     *
     * <h3>Key Methods:</h3>
     *
     * <ul>
     *     <li><b>{@code makeNext()}:</b> Retrieves the next key-value pair in the merged range, ensuring
     *     the result is within the specified range and boundary conditions.</li>
     *     <li><b>{@code initializeIterators()}:</b> Initializes the RocksDB iterators based on the specified range and direction.</li>
     *     <li><b>{@code isInRange()}:</b> Verifies if the current key-value pair is within the range defined by {@code from} and {@code to}.</li>
     *     <li><b>{@code fetchNextKeyValue()}:</b> Determines the next key-value pair to return based on the state of both iterators.</li>
     * </ul>
     *
     * <h3>Thread Safety:</h3>
     *
     * <p>This iterator is not thread-safe. If access from multiple threads is required, external synchronization must
     * be provided by the caller.</p>
     *
     * <p>The iterator is thread-safe for sequential operations but should not be accessed concurrently from multiple
     * threads without external synchronization.</p>
     *
     * <h2>Examples</h2>
     *
     * <h3>Iterate over a range:</h3>
     *
     * <pre>{@code
     * RocksIterator nonTimestampedIterator = accessor.newIterator(noTimestampColumnFamily);
     * RocksIterator timestampedIterator = accessor.newIterator(withTimestampColumnFamily);
     *
     * try (RocksDBDualCFRangeIterator iterator = new RocksDBDualCFRangeIterator(
     *         new Bytes("keyStart".getBytes()),
     *         new Bytes("keyEnd".getBytes()),
     *         nonTimestampedIterator,
     *         timestampedIterator,
     *         "storeName",
     *         true,  // Forward iteration
     *         true   // Inclusive upper boundary
     * )) {
     *     while (iterator.hasNext()) {
     *         KeyValue<Bytes, byte[]> entry = iterator.next();
     *         System.out.println("Key: " + entry.key + ", Value: " + Arrays.toString(entry.value));
     *     }
     * }
     * }</pre>
     *
     * <h2>Exceptions</h2>
     *
     * <ul>
     *     <li><b>{@link InvalidStateStoreException}:</b> Thrown if the iterator is accessed after being closed.</li>
     *     <li><b>{@link IllegalStateException}:</b> Thrown if the close callback is not properly set before usage.</li>
     * </ul>
     *
     * @see AbstractIterator
     * @see ManagedKeyValueIterator
     * @see RocksDBStore
     */
    private static class RocksDBDualCFRangeIterator extends AbstractIterator<KeyValue<Bytes, byte[]>> implements ManagedKeyValueIterator<Bytes, byte[]> {
        private Runnable closeCallback;
        private byte[] nonTimestampedNext;
        private byte[] timestampedNext;
        private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
        private final RocksIterator nonTimestampedIterator;
        private final RocksIterator timestampedIterator;
        private final String storeName;
        private final boolean forward;
        private final boolean toInclusive;
        private final byte[] rawLastKey;
        private volatile boolean open = true;

        private RocksDBDualCFRangeIterator(final Bytes from,
                                           final Bytes to,
                                           final RocksIterator nonTimestampedIterator,
                                           final RocksIterator timestampedIterator,
                                           final String storeName,
                                           final boolean forward,
                                           final boolean toInclusive) {
            this.forward = forward;
            this.nonTimestampedIterator = nonTimestampedIterator;
            this.storeName = storeName;
            this.toInclusive = toInclusive;
            this.timestampedIterator = timestampedIterator;

            this.rawLastKey = initializeIterators(from, to);
        }

        /**
         * Creates a new {@code RocksDBDualCFRangeIterator}.
         *
         * <p>Initializes the RocksDB iterators for two column families (timestamped and non-timestamped) and sets up
         * the range and direction for iteration.</p>
         *
         * @param from                   The starting key of the range. Can be {@code null} for an open range.
         * @param to                     The ending key of the range. Can be {@code null} for an open range.
         * @param nonTimestampedIterator The iterator for the non-timestamped column family.
         * @param timestampedIterator    The iterator for the timestamped column family.
         * @param storeName              The name of the store associated with this iterator.
         * @param forward                {@code true} for forward iteration; {@code false} for reverse iteration.
         * @param toInclusive            Whether the upper boundary of the range is inclusive.
         */
        public static RocksDBDualCFRangeIterator of(final Bytes from,
                                                    final Bytes to,
                                                    final RocksIterator nonTimestampedIterator,
                                                    final RocksIterator timestampedIterator,
                                                    final String storeName,
                                                    final boolean forward,
                                                    final boolean toInclusive) {
            final RocksDBDualCFRangeIterator iterator =
                new RocksDBDualCFRangeIterator(
                    from,
                    to,
                    nonTimestampedIterator,
                    timestampedIterator,
                    storeName,
                    forward,
                    toInclusive
                );
            iterator.initializeIterators(from, to);
            return iterator;
        }

        /**
         * Retrieves the next key-value pair in the range.
         *
         * <p>This method determines the next key-value pair to return by merging the results from the two column
         * families. If both column families have keys, it selects the one that matches the iteration order and range
         * conditions. Keys outside the specified range are skipped.</p>
         *
         * @return The next {@link KeyValue} pair in the range, or {@code null} if no more elements are available.
         */
        @Override
        protected KeyValue<Bytes, byte[]> makeNext() {
            loadNextKeys();
            if (nonTimestampedNext == null && timestampedNext == null) return allDone();
            final KeyValue<Bytes, byte[]> next = fetchNextKeyValue();
            return isInRange(next) ? next : allDone();
        }

        /**
         * Returns the next key in the range without advancing the iterator.
         *
         * <p>This method retrieves the key of the next {@link KeyValue} pair that would be returned by {@link #next()},
         * without moving the iterator forward. This is useful for inspecting the next key without affecting the
         * iterator's state.</p>
         *
         * @return The next key as a {@link Bytes} object.
         *
         * @throws NoSuchElementException If there are no more elements in the iterator.
         */
        @Override
        public Bytes peekNextKey() {
            if (!hasNext()) throw new NoSuchElementException();
            return super.peek().key;
        }

        /**
         * Advances the iterator and returns the next key-value pair.
         *
         * @return The next {@link KeyValue} pair in the range.
         *
         * @throws InvalidStateStoreException If the iterator has been closed.
         */
        @Override
        public synchronized KeyValue<Bytes, byte[]> next() {
            ensureOpen();
            return super.next();
        }

        /**
         * Checks if there are more elements available in the range.
         *
         * @return {@code true} if the iterator has more elements; {@code false} otherwise.
         *
         * @throws InvalidStateStoreException If the iterator has been closed.
         */
        @Override
        public synchronized boolean hasNext() {
            ensureOpen();
            return super.hasNext();
        }

        /**
         * Closes the iterator and releases associated resources.
         *
         * <p>This method ensures that the RocksDB iterators for both column families are properly closed. After this
         * method is called, any subsequent calls to {@link #hasNext()}, {@link #next()}, or {@link #peekNextKey()} will
         * result in an {@link InvalidStateStoreException}.</p>
         *
         * @throws IllegalStateException If the close callback has not been set before calling this method.
         */
        @Override
        public synchronized void close() {
            if (closeCallback == null) {
                final String message = "RocksDBDualCFIterator expects close callback to be set immediately upon creation";
                throw new IllegalStateException(message);
            }
            closeCallback.run();

            nonTimestampedIterator.close();
            timestampedIterator.close();
            open = false;
        }

        /**
         * Registers a callback to be executed when the iterator is closed.
         *
         * @param closeCallback A {@link Runnable} to execute during the {@link #close()} operation.
         */
        @Override
        public void onClose(final Runnable closeCallback) {
            this.closeCallback = closeCallback;
        }

        private KeyValue<Bytes, byte[]> compareAndHandleKeys() {
            final int comparison = comparator.compare(nonTimestampedNext, timestampedNext);
            if (forward ? comparison <= 0 : comparison >= 0) {
                return handleNonTimestampedOnly();
            } else {
                return handleTimestampedOnly();
            }
        }

        /**
         * Determines the next key-value pair to return.
         *
         * <p>If one of the column family iterators is exhausted, the method returns the result from the other iterator.
         * If both iterators have keys, the method compares the keys and returns the appropriate result based on the
         * iteration direction.</p>
         *
         * @return The next {@link KeyValue} pair to return.
         */
        private KeyValue<Bytes, byte[]> fetchNextKeyValue() {
            if (nonTimestampedNext == null) {
                return handleTimestampedOnly();
            } else if (timestampedNext == null) {
                return handleNonTimestampedOnly();
            } else {
                return compareAndHandleKeys();
            }
        }

        private KeyValue<Bytes, byte[]> handleNonTimestampedOnly() {
            final KeyValue<Bytes, byte[]> result = KeyValue.pair(new Bytes(nonTimestampedNext), convertToTimestampedFormat(nonTimestampedIterator.value()));
            moveIterator(nonTimestampedIterator);
            nonTimestampedNext = null;
            return result;
        }

        private KeyValue<Bytes, byte[]> handleTimestampedOnly() {
            final KeyValue<Bytes, byte[]> result = KeyValue.pair(new Bytes(timestampedNext), timestampedIterator.value());
            moveIterator(timestampedIterator);
            timestampedNext = null;
            return result;
        }

        /**
         * Checks if the given key-value pair is within the specified range.
         *
         * <p>The method compares the key against the range's upper boundary ({@code rawLastKey}) and determines if it
         * falls within the range.</p>
         *
         * @param keyValue The key-value pair to check.
         *
         * @return {@code true} if the key is within the range; {@code false} otherwise.
         */
        private boolean isInRange(final KeyValue<Bytes, byte[]> keyValue) {
            if (rawLastKey == null) return true; // Open-ended range
            final int comparison = comparator.compare(keyValue.key.get(), rawLastKey);
            return (toInclusive && comparison == 0) || (forward ? comparison < 0 : comparison > 0);
        }

        /**
         * Initializes the RocksDB iterators based on the specified range and direction.
         *
         * <p>This method positions the iterators at the starting point of the range and determines the raw byte
         * representation of the upper boundary (if provided).</p>
         *
         * @param from The starting key of the range. Can be {@code null} for an open range.
         * @param to   The ending key of the range. Can be {@code null} for an open range.
         *
         * @return The raw byte representation of the upper boundary, or {@code null} if no boundary is specified.
         */
        private byte[] initializeIterators(final Bytes from, final Bytes to) {
            if (forward) {
                seekIterator(from, timestampedIterator, true);
                seekIterator(from, nonTimestampedIterator, true);
                return to == null ? null : to.get();
            } else {
                seekIterator(to, timestampedIterator, false);
                seekIterator(to, nonTimestampedIterator, false);
                return from == null ? null : from.get();
            }
        }

        private void ensureOpen() {
            if (!open) {
                final String message = String.format("RocksDB iterator for store %s has closed", storeName);
                throw new InvalidStateStoreException(message);
            }
        }

        /**
         * Loads the next keys from the iterators if they are valid.
         *
         * <p>This method checks whether the next key for each column family is null. If the corresponding iterator is
         * valid, it fetches the next key.</p>
         */
        private void loadNextKeys() {
            if (nonTimestampedNext == null && nonTimestampedIterator.isValid()) nonTimestampedNext = nonTimestampedIterator.key();
            if (timestampedNext == null && timestampedIterator.isValid()) timestampedNext = timestampedIterator.key();
        }

        /**
         * Advances the given iterator based on the iteration direction.
         *
         * @param iterator The {@link RocksIterator} to advance.
         */
        private void moveIterator(final RocksIterator iterator) {
            if (forward) {
                iterator.next();
            } else {
                iterator.prev();
            }
        }

        /**
         * Seeks the iterator to the specified position.
         *
         * <p>If the target is {@code null}, the iterator is positioned at the start or end of the dataset, depending on
         * the direction.</p>
         *
         * @param iterator The {@link RocksIterator} to seek.
         * @param target   The target key to seek to. Can be {@code null}.
         * @param forward  {@code true} for forward iteration; {@code false} for reverse.
         */
        private void seekIterator(final Bytes target, final RocksIterator iterator, final boolean forward) {
            if (target == null) {
                if (forward) {
                    iterator.seekToFirst();
                } else {
                    iterator.seekToLast();
                }
            } else {
                if (forward) {
                    iterator.seek(target.get());
                } else {
                    iterator.seekForPrev(target.get());
                }
            }
        }
    }
}
