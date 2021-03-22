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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface TimeOrderedKeyValueBuffer<K, V> extends StateStore, ReadOnlyKeyValueStore<K, V> {

    final class Eviction<K, V> {
        private final K key;
        private final Change<V> value;
        private final ProcessorRecordContext recordContext;

        Eviction(final K key, final Change<V> value, final ProcessorRecordContext recordContext) {
            this.key = key;
            this.value = value;
            this.recordContext = recordContext;
        }

        public K key() {
            return key;
        }

        public Change<V> value() {
            return value;
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

    /**
     * In the case of join operations or the other materializations, the key/value serdes used by the statestores are
     * provided explicitly by the user (caller), through {@link org.apache.kafka.streams.kstream.Joined} or
     * {@link org.apache.kafka.streams.kstream.Materialized}.
     *
     * However, suppression operation does not have those kinds of instances. For this reason, the key/value serde can be null
     * when the suppression buffer instance is created. To make up for these shortcomings, the key/value serdes can be explicitly
     * set by the processor via this method.
     */
    void setSerdesIfNull(final Serde<K> keySerde, final Serde<V> valueSerde);

    void put(long time, K key, Change<V> value, ProcessorRecordContext recordContext);

    Maybe<ValueAndTimestamp<V>> priorValueForBuffered(K key);

    int evictWhile(final Supplier<Boolean> predicate, final Consumer<Eviction<K, V>> callback);

    int numRecords();

    long bufferSize();

    long minTimestamp();

    default long approximateNumEntries() {
        return numRecords();
    }
}
