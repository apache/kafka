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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper utility for managing the bytes layout of the value stored in segments of the {@link RocksDBVersionedStore}.
 * All record versions for the same key (in the same segment) are collected into a single row.
 * The value format for each row is:
 * <pre>
 *     <next_timestamp> + <min_timestamp> + <list of <timestamp, value_size>, reverse-sorted by timestamp> + <list of values, forward-sorted by timestamp>
 * </pre>
 * where:
 * <ul>
 * <li>{@code next_timestamp} is the validTo timestamp of the latest record version stored in this
 * row,</li>
 * <li>{@code min_timestamp} is the validFrom timestamp of the earliest record version stored
 * in this row, and</li>
 * <li>Negative {@code value_size} is used to indicate that the value stored is a tombstone,
 * in order to distinguish from empty array which has {@code value_size} of zero. In practice,
 * {@code value_size} is always set to -1 for the tombstone case, though this need not be true
 * in general.</li>
 * </ul>
 * <p>
 * Note that the value format above does not store the number of record versions contained in the
 * row. It is not necessary to store this information separately because this information is
 * never required on its own. Record versions are always deserialized in order, and we can
 * determine when we have reached the end of the list based on whether the (validFrom) timestamp of
 * the record version equals the {@code min_timestamp}.
 * <p>
 * There is one edge case with regards to the segment value format described above, which is useful
 * to know for understanding the code in this file, but not relevant for callers of the class.
 * Only continue reading if you want to understand details for reading the code in this file itself.
 * <p>
 * In the typical case, all record (validFrom) timestamps and the {@code next_timestamp} of the
 * segment row will form a strictly increasing sequence, i.e., it is not valid to have a record version
 * with validTo timestamp equal to (or less than) its validFrom timestamp. The one edge case /
 * exception occurs when the store contains no record versions (for a particular key) and a tombstone
 * is put. This case will result in a "degenerate" segment row containing the single tombstone, with both
 * {@code min_timestamp} and {@code next_timestamp} equal to the (validFrom) timestamp of the
 * tombstone. (The reverse-sorted list of {@code <timestamp, value_size>} in the row format will
 * still contain a single tombstone, as usual.)
 * <p>
 * The reason this happens is because tracking the tombstone is still required to maintain the correct
 * history for the key in question (in case an out-of-order record inserted to the store later
 * is deleted by this tombstone). As usual, this tombstone cannot be put into the latest value store,
 * as the latest value store has no expiry mechanism and we risk tombstones accumulating indefinitely.
 * What's different about this case is that usually we put the tombstone into a segment store as the
 * validTo timestamp of another record (the previous contents of the latest value store), but there
 * is no other record in this case. So, the tombstone is put into the segment as its own record,
 * with a well-defined validFrom timestamp but an undefined validTo. Because we can't use a validTo
 * of infinity to pick a segment, we instead use validTo as the tombstone's own timestamp as a
 * surrogate to select a segment. This works because querying for the latest record version as of a
 * later timestamp will correctly return that no record version is present
 * <p>
 * The same edge case also occurs if the store does not literally contain no existing record
 * versions (for the particular key), so long as no record versions are relevant for the put process.
 * Specifically, if the latest record version is already a tombstone and that tombstone is stored
 * in an older segment than is relevant for the timestamp of the new tombstone being put, then the
 * store "appears" empty during the put process for the new tombstone as the old segments are not
 * searched for the newer put. Note also that after a "degenerate" segment row has formed, it's possible
 * that the row will remain degenerate even as newer record versions are added. (For example,
 * if additional puts happen with later timestamps such that those puts only affect later segments,
 * then the earlier segment with the degenerate row will remain the same.)
 * <p>
 * Callers of this class need not concern themselves with this edge case / details of degenerate
 * segment rows because all the exposed methods function as expected, even in the degenerate case.
 * All methods may still be called, with the exception of {@link SegmentValue#find(long, boolean)}
 * and those that depend on it (i.e., {@link SegmentValue#updateRecord(long, byte[], int)} and
 * {@link SegmentValue#insert(long, byte[], int)}). Missing support for calling these methods on
 * degenerate rows is not an issue because the same timestamp bounds restrictions required for
 * calling {@link SegmentValue#find(long, boolean)} on regular segment rows serve to prevent callers
 * from calling the method on degenerate rows as well.
 */
final class RocksDBVersionedStoreSegmentValueFormatter {
    private static final int TIMESTAMP_SIZE = 8;
    private static final int VALUE_SIZE = 4;

    /**
     * @return the validTo timestamp of the latest record in the provided segment
     */
    static long getNextTimestamp(final byte[] segmentValue) {
        return ByteBuffer.wrap(segmentValue).getLong(0);
    }

    /**
     * @return the (validFrom) timestamp of the earliest record in the provided segment.
     */
    static long getMinTimestamp(final byte[] segmentValue) {
        return ByteBuffer.wrap(segmentValue).getLong(TIMESTAMP_SIZE);
    }

    /**
     * @return the deserialized segment value
     */
    static SegmentValue deserialize(final byte[] segmentValue) {
        return new PartiallyDeserializedSegmentValue(segmentValue);
    }

    /**
     * Creates a new segment value that contains the provided record.
     * <p>
     * This method may also be used to create a "degenerate" segment with {@code null} value and
     * {@code validFrom} timestamp equal to {@code validTo}. (For more on degenerate segments,
     * see the main javadoc for this class.)
     *
     * @param value the record value
     * @param validFrom the record's (validFrom) timestamp
     * @param validTo the record's validTo timestamp
     * @return the newly created segment value
     */
    static SegmentValue newSegmentValueWithRecord(
        final byte[] value, final long validFrom, final long validTo) {
        return new PartiallyDeserializedSegmentValue(value, validFrom, validTo);
    }

    interface SegmentValue {

        /**
         * Finds the latest record in this segment row with (validFrom) timestamp not exceeding the
         * provided timestamp bound. This method requires that the provided timestamp bound exists
         * in this segment, i.e., that the provided timestamp bound is at least {@code minTimestamp}
         * and is smaller than {@code nextTimestamp}.
         *
         * @param timestamp the timestamp to find
         * @param includeValue whether the value of the found record should be returned with the result
         * @return the record that is found
         * @throws IllegalArgumentException if the provided timestamp is not contained within this segment
         */
        SegmentSearchResult find(long timestamp, boolean includeValue);

        /**
         * Inserts the provided record into the segment as the latest record in the segment row.
         * <p>
         * It is the caller's responsibility to ensure that this action is desirable. In the event
         * that the new record's (validFrom) timestamp is smaller than the current
         * {@code nextTimestamp} of the segment row, the operation will still be performed, and the
         * row's existing contents will be truncated to ensure consistency of timestamps within
         * the segment row. This truncation behavior helps reconcile inconsistencies between different
         * segments, or between a segment and the latest value store, of a
         * {@link RocksDBVersionedStore} instance.
         *
         * @param validFrom the (validFrom) timestamp of the record to insert
         * @param validTo the validTo timestamp of the record to insert
         * @param value the value of the record to insert
         */
        void insertAsLatest(long validFrom, long validTo, byte[] value);

        /**
         * Inserts the provided record into the segment row as the earliest record in the row.
         * It is the caller's responsibility to ensure that this action is valid, i.e.,
         * that record's (validFrom) timestamp is smaller than the current {@code minTimestamp}
         * of the segment row.
         *
         * @param timestamp the (validFrom) timestamp of the record to insert
         * @param value the value of the record to insert
         */
        void insertAsEarliest(long timestamp, byte[] value);

        /**
         * Inserts the provided record into the segment row at the provided index. This operation
         * requires that {@link SegmentValue#find(long, boolean)} has already been called in order to deserialize
         * the relevant index (to insert into index n requires that index n has already been deserialized).
         * <p>
         * It is the caller's responsibility to ensure that this action makes sense, i.e., that the
         * insertion index is correct for the (validFrom) timestamp of the record being inserted.
         *
         * @param timestamp the (validFrom) timestamp of the record to insert
         * @param value the value of the record to insert
         * @param index the index that the newly inserted record should occupy
         * @throws IllegalArgumentException if the provided index is out of bounds, or if
         *         {@code find()} has not been called to deserialize the relevant index.
         */
        void insert(long timestamp, byte[] value, int index);

        /**
         * Updates the record at the provided index with the provided value and (validFrom)
         * timestamp. This operation requires that {@link SegmentValue#find(long, boolean)} has
         * already been called in order to deserialize the relevant index (i.e., the one being updated).
         * <p>
         * It is the caller's responsibility to ensure that this action makes sense, i.e., that the
         * updated (validFrom) timestamp does not violate timestamp order within the segment row.
         *
         * @param timestamp the updated record (validFrom) timestamp
         * @param value the updated record value
         * @param index the index of the record to update
         */
        void updateRecord(long timestamp, byte[] value, int index);

        /**
         * @return the bytes serialization for this segment value
         */
        byte[] serialize();

        class SegmentSearchResult {
            private final int index;
            private final long validFrom;
            private final long validTo;
            private final byte[] value;

            SegmentSearchResult(final int index, final long validFrom, final long validTo) {
                this(index, validFrom, validTo, null);
            }

            SegmentSearchResult(final int index, final long validFrom, final long validTo,
                                final byte[] value) {
                this.index = index;
                this.validFrom = validFrom;
                this.validTo = validTo;
                this.value = value;
            }

            int index() {
                return index;
            }

            long validFrom() {
                return validFrom;
            }

            long validTo() {
                return validTo;
            }

            /**
             * This value will be null if the caller did not specify that the value should be
             * included with the return result from {@link SegmentValue#find(long, boolean)}.
             * In this case, it is up to the caller to not call this method, as the "value"
             * returned will not be meaningful.
             */
            byte[] value() {
                return value;
            }
        }
    }

    private static class PartiallyDeserializedSegmentValue implements SegmentValue {
        private byte[] segmentValue;
        private long nextTimestamp;
        private long minTimestamp;
        private boolean isDegenerate;

        private int deserIndex = -1; // index up through which this segment has been deserialized (inclusive)
        private List<TimestampAndValueSize> unpackedReversedTimestampAndValueSizes;
        private List<Integer> cumulativeValueSizes; // ordered same as timestamp and value sizes (reverse time-sorted)

        private PartiallyDeserializedSegmentValue(final byte[] segmentValue) {
            this.segmentValue = segmentValue;
            this.nextTimestamp =
                RocksDBVersionedStoreSegmentValueFormatter.getNextTimestamp(segmentValue);
            this.minTimestamp =
                RocksDBVersionedStoreSegmentValueFormatter.getMinTimestamp(segmentValue);
            this.isDegenerate = nextTimestamp == minTimestamp;
            resetDeserHelpers();
        }

        private PartiallyDeserializedSegmentValue(
            final byte[] valueOrNull, final long validFrom, final long validTo) {
            initializeWithRecord(new ValueAndValueSize(valueOrNull), validFrom, validTo);
        }

        @Override
        public SegmentSearchResult find(final long timestamp, final boolean includeValue) {
            if (timestamp < minTimestamp) {
                throw new IllegalArgumentException("Timestamp is too small to be found in this segment.");
            }
            if (timestamp >= nextTimestamp) {
                throw new IllegalArgumentException("Timestamp is too large to be found in this segment.");
            }
            // for degenerate segments, minTimestamp == nextTimestamp, and we will always throw an exception
            // thus, we don't need to handle the degenerate case below

            long currNextTimestamp = nextTimestamp;
            long currTimestamp = -1L; // choose an invalid timestamp. if this is valid, this needs to be re-worked
            int currValueSize;
            int currIndex = 0;
            int cumValueSize = 0;
            while (currTimestamp != minTimestamp) {
                if (currIndex <= deserIndex) {
                    final TimestampAndValueSize curr = unpackedReversedTimestampAndValueSizes.get(currIndex);
                    currTimestamp = curr.timestamp;
                    currValueSize = curr.valueSize;
                    cumValueSize = cumulativeValueSizes.get(currIndex);
                } else {
                    final int timestampSegmentIndex = 2 * TIMESTAMP_SIZE + currIndex * (TIMESTAMP_SIZE + VALUE_SIZE);
                    currTimestamp = ByteBuffer.wrap(segmentValue).getLong(timestampSegmentIndex);
                    currValueSize = ByteBuffer.wrap(segmentValue).getInt(timestampSegmentIndex + TIMESTAMP_SIZE);
                    cumValueSize += Math.max(currValueSize, 0);

                    deserIndex = currIndex;
                    unpackedReversedTimestampAndValueSizes.add(new TimestampAndValueSize(currTimestamp, currValueSize));
                    cumulativeValueSizes.add(cumValueSize);
                }

                if (currTimestamp <= timestamp) {
                    // found result
                    if (includeValue) {
                        if (currValueSize >= 0) {
                            final byte[] value = new byte[currValueSize];
                            final int valueSegmentIndex = segmentValue.length - cumValueSize;
                            System.arraycopy(segmentValue, valueSegmentIndex, value, 0, currValueSize);
                            return new SegmentSearchResult(currIndex, currTimestamp, currNextTimestamp, value);
                        } else {
                            return new SegmentSearchResult(currIndex, currTimestamp, currNextTimestamp, null);
                        }
                    } else {
                        return new SegmentSearchResult(currIndex, currTimestamp, currNextTimestamp);
                    }
                }

                // prep for next iteration
                currNextTimestamp = currTimestamp;
                currIndex++;
            }

            throw new IllegalStateException("Search in segment expected to find result but did not.");
        }

        @Override
        public void insertAsLatest(final long validFrom, final long validTo, final byte[] valueOrNull) {
            final ValueAndValueSize value = new ValueAndValueSize(valueOrNull);

            if (validFrom < nextTimestamp) {
                // detected inconsistency edge case where older segment has [a,b) while newer store
                // has [a,c), due to [b,c) having failed to write to newer store.
                // remove entries from this store until the overlap is resolved.
                // TODO: will be implemented in a follow-up PR
                throw new UnsupportedOperationException("case not yet implemented");
            }

            if (nextTimestamp == validFrom) {
                // nextTimestamp is moved into segment automatically as record is added on top
                if (isDegenerate) {
                    initializeWithRecord(value, validFrom, validTo);
                } else {
                    doInsert(validFrom, value, 0);
                }
            } else {
                // move nextTimestamp into list as tombstone and add new record on top
                if (isDegenerate) {
                    initializeWithRecord(new ValueAndValueSize(null), nextTimestamp, validFrom);
                } else {
                    doInsert(nextTimestamp, new ValueAndValueSize(null), 0);
                }
                doInsert(validFrom, value, 0);
            }
            // update nextTimestamp
            nextTimestamp = validTo;
            ByteBuffer.wrap(segmentValue, 0, TIMESTAMP_SIZE).putLong(nextTimestamp);
        }

        @Override
        public void insertAsEarliest(final long timestamp, final byte[] valueOrNull) {
            final ValueAndValueSize value = new ValueAndValueSize(valueOrNull);

            if (isDegenerate) {
                initializeWithRecord(value, timestamp, nextTimestamp);
            } else {
                final int lastIndex = find(minTimestamp, false).index;
                doInsert(timestamp, value, lastIndex + 1);
            }
        }

        @Override
        public void insert(final long timestamp, final byte[] valueOrNull, final int index) {
            // public-facing method contains stricter index requirement than the internal helper
            // method doInsert() below
            if (index > deserIndex) {
                throw new IllegalArgumentException("Must invoke find() to deserialize record before insert() at specific index.");
            }

            final ValueAndValueSize value = new ValueAndValueSize(valueOrNull);
            doInsert(timestamp, value, index);
        }

        private void doInsert(final long timestamp, final ValueAndValueSize value, final int index) {
            if (index > deserIndex + 1) {
                throw new IllegalStateException("Must invoke find() to deserialize record before insert() at specific index.");
            }
            if (isDegenerate || index < 0) {
                throw new IllegalStateException("Cannot insert at negative index or into degenerate segment.");
            }

            final boolean needsMinTsUpdate = isLastIndex(index - 1);
            truncateDeserHelpersToIndex(index - 1);
            unpackedReversedTimestampAndValueSizes.add(new TimestampAndValueSize(timestamp, value.valueSize()));
            final int prevCumValueSize = deserIndex == -1 ? 0 : cumulativeValueSizes.get(deserIndex);
            cumulativeValueSizes.add(prevCumValueSize + value.value().length);
            deserIndex++;

            // update serialization and other props
            final int segmentTimestampIndex = 2 * TIMESTAMP_SIZE + index * (TIMESTAMP_SIZE + VALUE_SIZE);
            segmentValue = ByteBuffer.allocate(segmentValue.length + TIMESTAMP_SIZE + VALUE_SIZE + value.value().length)
                .put(segmentValue, 0, segmentTimestampIndex)
                .putLong(timestamp)
                .putInt(value.valueSize())
                .put(segmentValue, segmentTimestampIndex, segmentValue.length - segmentTimestampIndex - prevCumValueSize)
                .put(value.value())
                .put(segmentValue, segmentValue.length - prevCumValueSize, prevCumValueSize)
                .array();

            if (needsMinTsUpdate) {
                minTimestamp = timestamp;
                ByteBuffer.wrap(segmentValue, TIMESTAMP_SIZE, TIMESTAMP_SIZE).putLong(TIMESTAMP_SIZE, minTimestamp);
            }
        }

        @Override
        public void updateRecord(final long timestamp, final byte[] valueOrNull, final int index) {
            if (index > deserIndex || index < 0) {
                throw new IllegalArgumentException("Must invoke find() to deserialize record before updateRecord().");
            }
            final ValueAndValueSize value = new ValueAndValueSize(valueOrNull);

            final int oldValueSize = Math.max(unpackedReversedTimestampAndValueSizes.get(index).valueSize, 0);
            final int oldCumValueSize = cumulativeValueSizes.get(index);

            final boolean needsMinTsUpdate = isLastIndex(index);
            unpackedReversedTimestampAndValueSizes.set(index, new TimestampAndValueSize(timestamp, value.valueSize()));
            cumulativeValueSizes.set(index, oldCumValueSize - oldValueSize + value.value().length);
            truncateDeserHelpersToIndex(index);

            // update serialization and other props
            final int segmentTimestampIndex = 2 * TIMESTAMP_SIZE + index * (TIMESTAMP_SIZE + VALUE_SIZE);
            segmentValue = ByteBuffer.allocate(segmentValue.length - oldValueSize + value.value().length)
                .put(segmentValue, 0, segmentTimestampIndex)
                .putLong(timestamp)
                .putInt(value.valueSize())
                .put(segmentValue, segmentTimestampIndex + TIMESTAMP_SIZE + VALUE_SIZE, segmentValue.length - (segmentTimestampIndex + TIMESTAMP_SIZE + VALUE_SIZE) - oldCumValueSize)
                .put(value.value())
                .put(segmentValue, segmentValue.length - oldCumValueSize + oldValueSize, oldCumValueSize - oldValueSize)
                .array();

            if (needsMinTsUpdate) {
                minTimestamp = timestamp;
                ByteBuffer.wrap(segmentValue, TIMESTAMP_SIZE, TIMESTAMP_SIZE).putLong(TIMESTAMP_SIZE, minTimestamp);
            }
        }

        @Override
        public byte[] serialize() {
            return segmentValue;
        }

        private void initializeWithRecord(final ValueAndValueSize value, final long validFrom, final long validTo) {
            this.nextTimestamp = validTo;
            this.minTimestamp = validFrom;
            this.segmentValue = ByteBuffer.allocate(TIMESTAMP_SIZE * 3 + VALUE_SIZE + value.value().length)
                .putLong(nextTimestamp)
                .putLong(minTimestamp)
                .putLong(validFrom)
                .putInt(value.valueSize())
                .put(value.value())
                .array();
            this.isDegenerate = nextTimestamp == minTimestamp;
            resetDeserHelpers();
        }

        private void resetDeserHelpers() {
            deserIndex = -1;
            unpackedReversedTimestampAndValueSizes = new ArrayList<>();
            cumulativeValueSizes = new ArrayList<>();
        }

        private void truncateDeserHelpersToIndex(final int index) {
            deserIndex = index;
            unpackedReversedTimestampAndValueSizes.subList(index + 1, unpackedReversedTimestampAndValueSizes.size()).clear();
            cumulativeValueSizes.subList(index + 1, cumulativeValueSizes.size()).clear();
        }

        /**
         * This method can only be called if {@code index} has already been deserialized.
         */
        private boolean isLastIndex(final int index) {
            if (index < 0) {
                return false;
            }
            return unpackedReversedTimestampAndValueSizes.get(index).timestamp == minTimestamp;
        }

        private static class TimestampAndValueSize {
            final long timestamp;
            final int valueSize;

            TimestampAndValueSize(final long timestamp, final int valueSize) {
                this.timestamp = timestamp;
                this.valueSize = valueSize;
            }
        }

        /**
         * Helper class to assist in storing tombstones within segment values. Tombstones are stored
         * with {@code value_size} as a negative value, in order to disambiguate between tombstones
         * and empty bytes, as both have {@code value} as an empty array.
         */
        private static class ValueAndValueSize {
            private final byte[] valueToStore;
            private final int valueSizeToStore;

            ValueAndValueSize(final byte[] valueOrNull) {
                if (valueOrNull == null) {
                    valueToStore = new byte[0];
                    valueSizeToStore = -1;
                } else {
                    valueToStore = valueOrNull;
                    valueSizeToStore = valueToStore.length;
                }
            }

            /**
             * @return the value to be stored into the segment as part of the values list.
             *         This will never be null.
             */
            byte[] value() {
                return valueToStore;
            }

            /**
             * @return the value size to be stored into the segment as part of the timestamps
             *         and value sizes list. This will be negative for tombstones, and nonnegative
             *         otherwise.
             */
            int valueSize() {
                return valueSizeToStore;
            }
        }
    }
}