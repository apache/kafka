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

import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;
import org.rocksdb.RocksIterator;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RocksDBPrefixRangeIteratorTest {
    private final String storeName = "store";
    private final String key1 = "a";
    private final String key2 = "b";

    private final byte[] prefix1 = new byte[]{0x1};
    private final byte[] prefix2 = new byte[]{0x2};

    private final String value = "value";
    private final byte[] valueBytes = value.getBytes();

    private final Bytes prefix1Key1Bytes = Bytes.wrap(concat(prefix1, key1.getBytes()));
    private final Bytes prefix1Key2Bytes = Bytes.wrap(concat(prefix1, key2.getBytes()));
    private final Bytes prefix2Key1Bytes = Bytes.wrap(concat(prefix2, key1.getBytes()));
    private final Bytes prefix2Key2Bytes = Bytes.wrap(concat(prefix2, key2.getBytes()));

    private final Bytes prefix1Bytes = Bytes.wrap(prefix1);
    private final Bytes prefix2Bytes = Bytes.wrap(prefix2);

    private byte[] concat(final byte[] a, final byte[] b) {
        final ByteBuffer concatArrays = ByteBuffer.allocate(a.length + b.length);
        concatArrays.put(a);
        concatArrays.put(b);
        return concatArrays.array();
    }


    @Test
    public void shouldReturnAllKeysInThePrefixRangeInForwardDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(prefix1);
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(prefix1Key1Bytes.get())
            .andReturn(prefix1Key2Bytes.get())
            .andReturn(prefix2Key1Bytes.get())
            .andReturn(prefix2Key2Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(4);
        rocksIterator.next();
        expectLastCall().times(4);
        replay(rocksIterator);

        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            prefix2Bytes,
            true,
            true
        );

        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key2Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key2Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysInThePrefixRangeReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seekForPrev(prefix2);
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(prefix2Key2Bytes.get())
            .andReturn(prefix2Key1Bytes.get())
            .andReturn(prefix1Key2Bytes.get())
            .andReturn(prefix1Key1Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(4);
        rocksIterator.prev();
        expectLastCall().times(4);
        replay(rocksIterator);

        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            prefix2Bytes,
            false,
            true
        );

        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key2Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key2Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysWhenLastPrefixKeyIsGreaterThanLargestKeyInStateStoreInForwardDirection() {
        final Bytes toBytes = Bytes.increment(prefix2Bytes);
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(prefix1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(prefix1Key1Bytes.get())
            .andReturn(prefix1Key2Bytes.get())
            .andReturn(prefix2Key1Bytes.get())
            .andReturn(prefix2Key2Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(4);
        rocksIterator.next();
        expectLastCall().times(4);
        replay(rocksIterator);
        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            toBytes,
            true,
            true
        );
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key2Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key2Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnNoKeysWhenLastPrefixKeyIsSmallerThanSmallestKeyInStateStoreForwardDirection() {
        // key range in state store: [c-f]
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(prefix1Bytes.get());
        expect(rocksIterator.isValid()).andReturn(false);
        replay(rocksIterator);
        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            prefix2Bytes,
            true,
            true
        );
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }


    @Test
    public void shouldReturnNoKeysWhenLastPrefixKeyIsLargerThanLargestKeyInStateStoreReverseDirection() {
        // key range in state store: [c-f]
        final byte[] fromPrefix = new byte[]{0x1};
        final byte[] toPrefix = new byte[]{0x2};
        final  Bytes fromBytes = Bytes.wrap(fromPrefix);
        final  Bytes toBytes = Bytes.wrap(toPrefix);
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seekForPrev(toBytes.get());
        expect(rocksIterator.isValid())
            .andReturn(false);
        replay(rocksIterator);
        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            fromBytes,
            toBytes,
            false,
            true
        );
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysInPartiallyOverlappingPrefixRangeInForwardDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(prefix1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(prefix2Key1Bytes.get())
            .andReturn(prefix2Key2Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.next();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            prefix2Bytes,
            true,
            true
        );
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key2Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysInPartiallyOverlappingPrefixRangeInReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        final byte[] toPrefix = new byte[]{0x3};
        final Bytes toBytes = Bytes.wrap(toPrefix);
        rocksIterator.seekForPrev(toBytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(prefix2Key1Bytes.get())
            .andReturn(prefix1Key1Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.prev();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            toBytes,
            false,
            true
        );
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix2Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldCloseIterator() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(prefix1Bytes.get());
        rocksIterator.close();
        expectLastCall().times(1);
        replay(rocksIterator);
        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            prefix2Bytes,
            true,
            true
        );
        rocksDBPrefixRangeIterator.close();
        verify(rocksIterator);
    }

    @Test
    public void shouldExcludeEndOfRange() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(prefix1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true);
        expect(rocksIterator.key())
            .andReturn(prefix1Key1Bytes.get())
            .andReturn(prefix2Key1Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.next();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBPrefixRangeIterator rocksDBPrefixRangeIterator = new RocksDBPrefixRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            prefix1Bytes,
            prefix2Bytes,
            true,
            false
        );
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(true));
        assertThat(rocksDBPrefixRangeIterator.next().key, is(prefix1Key1Bytes));
        assertThat(rocksDBPrefixRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }
}
