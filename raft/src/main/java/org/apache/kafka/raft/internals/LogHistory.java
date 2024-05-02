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
package org.apache.kafka.raft.internals;

import java.util.Objects;
import java.util.Optional;

/**
 * An object that tracks values of {@code T} at different offsets.
 */
public interface LogHistory<T> {
    /**
     * Add a new value at a given offset.
     *
     * The provided {@code offset} must be greater than or equal to 0 and must be greater than the
     * offset of all previous calls to this method.
     *
     * @param offset the offset
     * @param value the value to store
     * @throws IllegalArgumentException if the offset is not greater than all previous offsets
     */
    void addAt(long offset, T value);

    /**
     * Returns the value that has the largest offset that is less than or equal to the provided
     * offset.
     *
     * @param offset the offset
     * @return the value if it exists, otherwise {@code Optional.empty()}
     */
    Optional<T> valueAtOrBefore(long offset);

    /**
     * Returns the value with the largest offset.
     *
     * @return the value if it exists, otherwise {@code Optional.empty()}
     */
    Optional<Entry<T>> lastEntry();

    /**
     * Removes all entries with an offset greater than or equal to {@code endOffset}.
     *
     * @param endOffset the ending offset
     */
    void truncateNewEntries(long endOffset);

    /**
     * Removes all entries but the last entry that has an offset that is less than or equal to
     * {@code startOffset}.
     *
     * This operation does not remove the entry with the largest offset that is less than or equal
     * to {@code startOffset}. This is needed so that calls to {@code valueAtOrBefore} and
     * {@code lastEntry} always return a non-empty value if a value was previously added to this
     * object.
     *
     * @param startOffset the starting offset
     */
    void truncateOldEntries(long startOffset);

    /**
     * Removes all of the values from this object.
     */
    void clear();

    final static class Entry<T> {
        private final long offset;
        private final T value;

        public Entry(long offset, T value) {
            this.offset = offset;
            this.value = value;
        }

        public long offset() {
            return offset;
        }

        public T value() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry<?> that = (Entry<?>) o;

            if (offset != that.offset) return false;
            if (!Objects.equals(value, that.value)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return Objects.hash(offset, value);
        }

        @Override
        public String toString() {
            return String.format("Entry(offset=%d, value=%s)", offset, value);
        }
    }
}
