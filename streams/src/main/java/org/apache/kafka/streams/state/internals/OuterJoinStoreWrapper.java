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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.DslStoreFormat;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.AbstractConfigurableStoreFactory;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Wraps the outer-join store used by {@code KStreamKStreamJoin} so the processor only deals
 * with a single value shape: {@code AggregationWithHeaders<LeftOrRightValue<VLeft, VRight>>}.
 * <p>
 * The value carries no timestamp: the entry timestamp is part of the key
 * ({@link TimestampedKeyAndJoinSide#timestamp()}), which is what the entries are sorted by, so a
 * separate value-side timestamp would be redundant.
 * <p>
 * The underlying store is one of:
 * <ul>
 *   <li>plain: {@code KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>>}</li>
 *   <li>headers-aware: {@code KeyValueStore<TimestampedKeyAndJoinSide<K>, AggregationWithHeaders<LeftOrRightValue<VLeft, VRight>>>}</li>
 * </ul>
 * Both variants are wrapped by the same {@link MeteredKeyValueStore} class — they only differ
 * in the value serde and (erased) generic parameter — so the variant cannot be detected by
 * casting the runtime store. Instead, the variant is read from the {@link StoreFactory}'s
 * configured {@link DslStoreFormat}.
 */
public class OuterJoinStoreWrapper<K, VLeft, VRight> {

    private final boolean isHeadersStore;
    private KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>> plainStore;
    private KeyValueStore<TimestampedKeyAndJoinSide<K>, AggregationWithHeaders<LeftOrRightValue<VLeft, VRight>>> headersStore;

    public OuterJoinStoreWrapper(final ProcessorContext<?, ?> context, final StoreFactory storeFactory) {
        this.isHeadersStore = isHeadersAware(storeFactory);
        if (isHeadersStore) {
            headersStore = context.getStateStore(storeFactory.storeName());
        } else {
            plainStore = context.getStateStore(storeFactory.storeName());
        }
    }

    private static boolean isHeadersAware(final StoreFactory storeFactory) {
        if (storeFactory instanceof AbstractConfigurableStoreFactory) {
            return ((AbstractConfigurableStoreFactory) storeFactory).dslStoreFormat() == DslStoreFormat.HEADERS;
        }
        return false;
    }

    public boolean isHeadersStore() {
        return isHeadersStore;
    }

    public void put(final TimestampedKeyAndJoinSide<K> key,
                    final LeftOrRightValue<VLeft, VRight> value,
                    final Headers headers) {
        if (headersStore != null) {
            headersStore.put(key, value == null
                ? null
                : AggregationWithHeaders.makeAllowNullable(value, headers));
        } else {
            plainStore.put(key, value);
        }
    }

    public void putIfAbsent(final TimestampedKeyAndJoinSide<K> key,
                            final LeftOrRightValue<VLeft, VRight> value,
                            final Headers headers) {
        if (headersStore != null) {
            headersStore.putIfAbsent(key, value == null
                ? null
                : AggregationWithHeaders.makeAllowNullable(value, headers));
        } else {
            plainStore.putIfAbsent(key, value);
        }
    }

    public KeyValueIterator<TimestampedKeyAndJoinSide<K>, AggregationWithHeaders<LeftOrRightValue<VLeft, VRight>>> all() {
        if (headersStore != null) {
            return headersStore.all();
        }
        // The plain store has no per-element headers. Callers that need the timestamp read it from
        // the key (TimestampedKeyAndJoinSide#timestamp). Callers that need the headers gate on
        // isHeadersStore() — the lifted headers slot here is unused on the plain path.
        return new LiftingIterator<>(plainStore.all());
    }

    private static final class LiftingIterator<K, V>
        implements KeyValueIterator<TimestampedKeyAndJoinSide<K>, AggregationWithHeaders<V>> {

        private final KeyValueIterator<TimestampedKeyAndJoinSide<K>, V> inner;

        LiftingIterator(final KeyValueIterator<TimestampedKeyAndJoinSide<K>, V> inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            inner.close();
        }

        @Override
        public TimestampedKeyAndJoinSide<K> peekNextKey() {
            return inner.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public KeyValue<TimestampedKeyAndJoinSide<K>, AggregationWithHeaders<V>> next() {
            final KeyValue<TimestampedKeyAndJoinSide<K>, V> kv = inner.next();
            final AggregationWithHeaders<V> lifted = kv.value == null
                ? null
                : AggregationWithHeaders.makeAllowNullable(kv.value, null);
            return KeyValue.pair(kv.key, lifted);
        }
    }
}
