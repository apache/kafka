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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** An interface for a time ordered buffer.
 *
 * @param <K> Key type
 * @param <V> Value type
 * @param <T> Store type
 */
public interface TimeOrderedKeyValueBuffer<K, V, T> extends StateStore {

    final class Eviction<K, T> {
        private final K key;
        private final T value;
        private final ProcessorRecordContext recordContext;

        Eviction(final K key, final T value, final ProcessorRecordContext recordContext) {
            this.key = key;
            this.value = value;
            this.recordContext = recordContext;
        }

        public K key() {
            return key;
        }

        public T value() {
            return value;
        }

        public Record<K, T> record() {
            return new Record<>(key, value, recordContext.timestamp());
        }

        public ProcessorRecordContext recordContext() {
            return recordContext;
        }

        @Override
        public String toString() {
            return "Eviction{key=" + key + ", value=" + value + ", recordContext=" + recordContext + '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Eviction<?, ?> eviction = (Eviction<?, ?>) o;
            return Objects.equals(key, eviction.key) &&
                Objects.equals(value, eviction.value) &&
                Objects.equals(recordContext, eviction.recordContext);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value, recordContext);
        }

    }

    void setSerdesIfNull(final SerdeGetter getter);

    void evictWhile(final Supplier<Boolean> predicate, final Consumer<Eviction<K, T>> callback);

    Maybe<ValueAndTimestamp<V>> priorValueForBuffered(K key);

    boolean put(long time, Record<K, T> record, ProcessorRecordContext recordContext);

    int numRecords();

    long bufferSize();

    long minTimestamp();
}
