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

import java.util.Collection;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.MergedSortedCacheWindowStoreKeyValueIterator.StoreKeyToWindowKey;
import org.apache.kafka.streams.state.internals.MergedSortedCacheWindowStoreKeyValueIterator.WindowKeyToBytes;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.KeyFirstWindowKeySchema;
import org.apache.kafka.streams.state.internals.PrefixedWindowKeySchemas.TimeFirstWindowKeySchema;
import org.apache.kafka.test.KeyValueIteratorStub;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MergedSortedCacheWrappedWindowStoreKeyValueIteratorTest {

    @FunctionalInterface
    private interface StoreKeySerializer<K> {
        Bytes serialize(final Windowed<K> key, final int seq, final StateSerdes<K, ?> serdes);
    }

    private static final SegmentedCacheFunction SINGLE_SEGMENT_CACHE_FUNCTION = new SegmentedCacheFunction(null, -1) {
        @Override
        public long segmentId(final Bytes key) {
            return 0;
        }
    };

    private static final int WINDOW_SIZE = 10;

    private final String storeKey = "a";
    private final String cacheKey = "b";

    private final TimeWindow storeWindow = new TimeWindow(0, 1);
    private final Iterator<KeyValue<Windowed<Bytes>, byte[]>> storeKvs = Collections.singleton(
        KeyValue.pair(new Windowed<>(Bytes.wrap(storeKey.getBytes()), storeWindow), storeKey.getBytes())).iterator();
    private final TimeWindow cacheWindow = new TimeWindow(10, 20);
    private Iterator<KeyValue<Bytes, LRUCacheEntry>> cacheKvs;
    final private Deserializer<String> deserializer = Serdes.String().deserializer();

    private StoreKeySerializer<String> storeKeySerializer;
    private StoreKeyToWindowKey storeKeyToWindowKey;
    private WindowKeyToBytes windowKeyToBytes;

    private enum SchemaType {
        WINDOW_KEY_SCHEMA,
        KEY_FIRST_SCHEMA,
        TIME_FIRST_SCHEMA
    }

    @Parameter
    public SchemaType schemaType;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][] {
            {SchemaType.WINDOW_KEY_SCHEMA},
            {SchemaType.KEY_FIRST_SCHEMA},
            {SchemaType.TIME_FIRST_SCHEMA},
        });
    }

    @Before
    public void setUp() {
        switch (schemaType) {
            case KEY_FIRST_SCHEMA:
                storeKeySerializer = KeyFirstWindowKeySchema::toStoreKeyBinary;
                storeKeyToWindowKey = KeyFirstWindowKeySchema::fromStoreKey;
                windowKeyToBytes = KeyFirstWindowKeySchema::toStoreKeyBinary;
                break;
            case WINDOW_KEY_SCHEMA:
                storeKeySerializer = WindowKeySchema::toStoreKeyBinary;
                storeKeyToWindowKey = WindowKeySchema::fromStoreKey;
                windowKeyToBytes = WindowKeySchema::toStoreKeyBinary;
                break;
            case TIME_FIRST_SCHEMA:
                storeKeySerializer = TimeFirstWindowKeySchema::toStoreKeyBinary;
                storeKeyToWindowKey = TimeFirstWindowKeySchema::fromStoreKey;
                windowKeyToBytes = TimeFirstWindowKeySchema::toStoreKeyBinary;
                break;
            default:
                throw new IllegalStateException("Unknown schemaType: " + schemaType);
        }
        cacheKvs = Collections.singleton(
            KeyValue.pair(
                SINGLE_SEGMENT_CACHE_FUNCTION.cacheKey(storeKeySerializer.serialize(
                    new Windowed<>(cacheKey, cacheWindow), 0, new StateSerdes<>("dummy", Serdes.String(), Serdes.ByteArray()))
                ),
                new LRUCacheEntry(cacheKey.getBytes())
            )
        ).iterator();
    }

    @Test
    public void shouldHaveNextFromStore() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator(), false);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldHaveNextFromReverseStore() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator(), true);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldGetNextFromStore() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator(), false);
        assertThat(convertKeyValuePair(mergeIterator.next()), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey)));
    }

    @Test
    public void shouldGetNextFromReverseStore() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator(), true);
        assertThat(convertKeyValuePair(mergeIterator.next()), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey)));
    }

    @Test
    public void shouldPeekNextKeyFromStore() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator(), false);
        assertThat(convertWindowedKey(mergeIterator.peekNextKey()), equalTo(new Windowed<>(storeKey, storeWindow)));
    }

    @Test
    public void shouldPeekNextKeyFromReverseStore() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(storeKvs, Collections.emptyIterator(), true);
        assertThat(convertWindowedKey(mergeIterator.peekNextKey()), equalTo(new Windowed<>(storeKey, storeWindow)));
    }

    @Test
    public void shouldHaveNextFromCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs, false);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldHaveNextFromReverseCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs, true);
        assertTrue(mergeIterator.hasNext());
    }

    @Test
    public void shouldGetNextFromCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs, false);
        assertThat(convertKeyValuePair(mergeIterator.next()), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
    }

    @Test
    public void shouldGetNextFromReverseCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs, true);
        assertThat(convertKeyValuePair(mergeIterator.next()), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
    }

    @Test
    public void shouldPeekNextKeyFromCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs, false);
        assertThat(convertWindowedKey(mergeIterator.peekNextKey()), equalTo(new Windowed<>(cacheKey, cacheWindow)));
    }

    @Test
    public void shouldPeekNextKeyFromReverseCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator mergeIterator =
            createIterator(Collections.emptyIterator(), cacheKvs, true);
        assertThat(convertWindowedKey(mergeIterator.peekNextKey()), equalTo(new Windowed<>(cacheKey, cacheWindow)));
    }

    @Test
    public void shouldIterateBothStoreAndCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator iterator = createIterator(storeKvs, cacheKvs, true);
        assertThat(convertKeyValuePair(iterator.next()), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey)));
        assertThat(convertKeyValuePair(iterator.next()), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReverseIterateBothStoreAndCache() {
        final MergedSortedCacheWindowStoreKeyValueIterator iterator = createIterator(storeKvs, cacheKvs, false);
        assertThat(convertKeyValuePair(iterator.next()), equalTo(KeyValue.pair(new Windowed<>(cacheKey, cacheWindow), cacheKey)));
        assertThat(convertKeyValuePair(iterator.next()), equalTo(KeyValue.pair(new Windowed<>(storeKey, storeWindow), storeKey)));
        assertFalse(iterator.hasNext());
    }

    private KeyValue<Windowed<String>, String> convertKeyValuePair(final KeyValue<Windowed<Bytes>, byte[]> next) {
        final String value = deserializer.deserialize("", next.value);
        return KeyValue.pair(convertWindowedKey(next.key), value);
    }

    private Windowed<String> convertWindowedKey(final Windowed<Bytes> bytesWindowed) {
        final String key = deserializer.deserialize("", bytesWindowed.key().get());
        return new Windowed<>(key, bytesWindowed.window());
    }


    private MergedSortedCacheWindowStoreKeyValueIterator createIterator(final Iterator<KeyValue<Windowed<Bytes>, byte[]>> storeKvs,
                                                                        final Iterator<KeyValue<Bytes, LRUCacheEntry>> cacheKvs,
                                                                        final boolean forward) {
        final DelegatingPeekingKeyValueIterator<Windowed<Bytes>, byte[]> storeIterator =
            new DelegatingPeekingKeyValueIterator<>("store", new KeyValueIteratorStub<>(storeKvs));

        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator =
            new DelegatingPeekingKeyValueIterator<>("cache", new KeyValueIteratorStub<>(cacheKvs));
        return new MergedSortedCacheWindowStoreKeyValueIterator(
            cacheIterator,
            storeIterator,
            new StateSerdes<>("name", Serdes.Bytes(), Serdes.ByteArray()),
            WINDOW_SIZE,
            SINGLE_SEGMENT_CACHE_FUNCTION,
            forward,
            storeKeyToWindowKey,
            windowKeyToBytes
        );
    }
}
