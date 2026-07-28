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
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.DslStoreFormat;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.StoreFactory;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

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
 * Both variants are wrapped by the same {@link MeteredKeyValueStore} class — they only differ in the
 * value serde and (erased) generic parameter — so the variant cannot be read off the outermost store.
 * It is read instead from the innermost bytes store, which carries the
 * {@link HeadersAwareListValueStore} marker exactly when the elements are in the headers format.
 * <p>
 * That is deliberately the <em>same</em> signal {@code OuterStreamJoinStoreFactory} uses to pick the
 * element serde and {@link ListValueStoreBuilder} uses to pick the changelogger, rather than the
 * configured {@link DslStoreFormat}: HEADERS only takes effect if the supplier could actually provide a
 * headers-aware bytes store, so keying off the format would claim headers for an in-memory or
 * user-supplied store that is in fact holding PLAIN elements.
 */
public class OuterJoinStoreWrapper<K, VLeft, VRight> {

    /**
     * The headers every entry on the plain path is lifted with. Shared rather than allocated per entry:
     * the flush loop never reads them back (it gates on {@link #isHeadersStore()}), and read-only means
     * that a future caller which does read them cannot mutate what every other entry sees.
     */
    private static final RecordHeaders EMPTY_HEADERS = new RecordHeaders();

    static {
        EMPTY_HEADERS.setReadOnly();
    }

    private final boolean isHeadersStore;
    private KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>> plainStore;
    private KeyValueStore<TimestampedKeyAndJoinSide<K>, AggregationWithHeaders<LeftOrRightValue<VLeft, VRight>>> headersStore;

    @SuppressWarnings("unchecked")
    public OuterJoinStoreWrapper(final ProcessorContext<?, ?> context, final StoreFactory storeFactory) {
        final StateStore store = context.getStateStore(storeFactory.storeName());
        this.isHeadersStore = WrappedStateStore.isHeadersAwareListValue(store);
        if (isHeadersStore) {
            headersStore = (KeyValueStore<TimestampedKeyAndJoinSide<K>, AggregationWithHeaders<LeftOrRightValue<VLeft, VRight>>>) store;
        } else {
            plainStore = (KeyValueStore<TimestampedKeyAndJoinSide<K>, LeftOrRightValue<VLeft, VRight>>) store;
        }
    }

    public boolean isHeadersStore() {
        return isHeadersStore;
    }

    /**
     * Appends {@code value} to the list held under {@code key}. Both arguments are required — the
     * whole-list delete is {@link #deleteList(TimestampedKeyAndJoinSide)}, not a null value.
     */
    public void put(final TimestampedKeyAndJoinSide<K> key,
                    final LeftOrRightValue<VLeft, VRight> value,
                    final Headers headers) {
        Objects.requireNonNull(value, "value must not be null; use deleteList to remove the list");
        // AggregationWithHeaders rejects null headers, but only once the value is non-null, so without
        // this the mistake would surface as an NPE from inside the value class instead of from here.
        Objects.requireNonNull(headers, "headers must not be null");
        if (headersStore != null) {
            headersStore.put(key, AggregationWithHeaders.make(value, headers));
        } else {
            plainStore.put(key, value);
        }
    }

    /**
     * Removes every value held under {@code key}. Spelled as a delete rather than a null put because
     * {@link ListValueStore#put(org.apache.kafka.common.utils.Bytes, byte[])} with a null value drops the
     * key's whole list, not just one element.
     */
    public void deleteList(final TimestampedKeyAndJoinSide<K> key) {
        if (headersStore != null) {
            headersStore.put(key, null);
        } else {
            plainStore.put(key, null);
        }
    }

    /**
     * Removes every value held under {@code key}, but only if the key has any — a no-op otherwise, so
     * that no tombstone is written for a key that was never there.
     */
    public void deleteListIfPresent(final TimestampedKeyAndJoinSide<K> key) {
        if (headersStore != null) {
            headersStore.putIfAbsent(key, null);
        } else {
            plainStore.putIfAbsent(key, null);
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
            final AggregationWithHeaders<V> lifted = AggregationWithHeaders.make(kv.value, EMPTY_HEADERS);
            return KeyValue.pair(kv.key, lifted);
        }
    }
}
