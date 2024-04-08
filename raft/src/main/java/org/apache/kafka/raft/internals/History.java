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

// TODO: document this type
public interface History<T> {
    public void addAt(long offset, T newValue);

    public Optional<T> valueAt(long offset);

    public Optional<Entry<T>> lastEntry();

    public void truncateTo(long endOffset);

    public void trimPrefixTo(long startOffset);

    public void clear();

    final public static class Entry<T> {
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

            Entry that = (Entry) o;

            if (offset != that.offset) return false;
            if (value != that.value) return false;

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
