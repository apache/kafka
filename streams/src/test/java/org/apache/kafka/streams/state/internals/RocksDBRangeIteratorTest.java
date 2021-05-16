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

import java.util.Collections;
import java.util.NoSuchElementException;

import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;

public class RocksDBRangeIteratorTest {

    private final String storeName = "store";
    private final String key1 = "a";
    private final String key2 = "b";
    private final String key3 = "c";
    private final String key4 = "d";

    private final String value = "value";
    private final  Bytes key1Bytes = Bytes.wrap(key1.getBytes());
    private final  Bytes key2Bytes = Bytes.wrap(key2.getBytes());
    private final  Bytes key3Bytes = Bytes.wrap(key3.getBytes());
    private final  Bytes key4Bytes = Bytes.wrap(key4.getBytes());
    private final byte[] valueBytes = value.getBytes();

    @Test
    public void shouldReturnAllKeysInTheRangeInForwardDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(key1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key1Bytes.get())
            .andReturn(key2Bytes.get())
            .andReturn(key3Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(3);
        rocksIterator.next();
        expectLastCall().times(3);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key3Bytes,
            true,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key1Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key2Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysInTheRangeReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seekForPrev(key3Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key3Bytes.get())
            .andReturn(key2Bytes.get())
            .andReturn(key1Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(3);
        rocksIterator.prev();
        expectLastCall().times(3);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key3Bytes,
            false,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key2Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key1Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysWhenLastKeyIsGreaterThanLargestKeyInStateStoreInForwardDirection() {
        final Bytes toBytes = Bytes.increment(key4Bytes);
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(key1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key1Bytes.get())
            .andReturn(key2Bytes.get())
            .andReturn(key3Bytes.get())
            .andReturn(key4Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(4);
        rocksIterator.next();
        expectLastCall().times(4);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            toBytes,
            true,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key1Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key2Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key4Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }


    @Test
    public void shouldReturnAllKeysWhenLastKeyIsSmallerThanSmallestKeyInStateStoreInReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seekForPrev(key4Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key4Bytes.get())
            .andReturn(key3Bytes.get())
            .andReturn(key2Bytes.get())
            .andReturn(key1Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(4);
        rocksIterator.prev();
        expectLastCall().times(4);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key4Bytes,
            false,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key4Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key2Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key1Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }


    @Test
    public void shouldReturnNoKeysWhenLastKeyIsSmallerThanSmallestKeyInStateStoreForwardDirection() {
        // key range in state store: [c-f]
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(key1Bytes.get());
        expect(rocksIterator.isValid()).andReturn(false);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key2Bytes,
            true,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnNoKeysWhenLastKeyIsLargerThanLargestKeyInStateStoreReverseDirection() {
        // key range in state store: [c-f]
        final String from = "g";
        final String to = "h";
        final  Bytes fromBytes = Bytes.wrap(from.getBytes());
        final  Bytes toBytes = Bytes.wrap(to.getBytes());
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seekForPrev(toBytes.get());
        expect(rocksIterator.isValid())
            .andReturn(false);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            fromBytes,
            toBytes,
            false,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysInPartiallyOverlappingRangeInForwardDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(key1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key2Bytes.get())
            .andReturn(key3Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.next();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key3Bytes,
            true,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key2Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnAllKeysInPartiallyOverlappingRangeInReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        final String to = "e";
        final Bytes toBytes = Bytes.wrap(to.getBytes());
        rocksIterator.seekForPrev(toBytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key4Bytes.get())
            .andReturn(key3Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.prev();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key3Bytes,
            toBytes,
            false,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key4Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnTheCurrentKeyOnInvokingPeekNextKeyInForwardDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(key1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key2Bytes.get())
            .andReturn(key3Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.next();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key3Bytes,
            true,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key2Bytes));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key2Bytes));
        assertThat(rocksDBRangeIterator.next().key, is(key2Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key3Bytes));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key3Bytes));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        assertThrows(NoSuchElementException.class, rocksDBRangeIterator::peekNextKey);
        verify(rocksIterator);
    }

    @Test
    public void shouldReturnTheCurrentKeyOnInvokingPeekNextKeyInReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        final Bytes toBytes = Bytes.increment(key4Bytes);
        rocksIterator.seekForPrev(toBytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true)
            .andReturn(false);
        expect(rocksIterator.key())
            .andReturn(key4Bytes.get())
            .andReturn(key3Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.prev();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key3Bytes,
            toBytes,
            false,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key4Bytes));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key4Bytes));
        assertThat(rocksDBRangeIterator.next().key, is(key4Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key3Bytes));
        assertThat(rocksDBRangeIterator.peekNextKey(), is(key3Bytes));
        assertThat(rocksDBRangeIterator.next().key, is(key3Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        assertThrows(NoSuchElementException.class, rocksDBRangeIterator::peekNextKey);
        verify(rocksIterator);
    }

    @Test
    public void shouldCloseIterator() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(key1Bytes.get());
        rocksIterator.close();
        expectLastCall().times(1);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key2Bytes,
            true,
            true
        );
        rocksDBRangeIterator.close();
        verify(rocksIterator);
    }

    @Test
    public void shouldExcludeEndOfRange() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        rocksIterator.seek(key1Bytes.get());
        expect(rocksIterator.isValid())
            .andReturn(true)
            .andReturn(true);
        expect(rocksIterator.key())
            .andReturn(key1Bytes.get())
            .andReturn(key2Bytes.get());
        expect(rocksIterator.value()).andReturn(valueBytes).times(2);
        rocksIterator.next();
        expectLastCall().times(2);
        replay(rocksIterator);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            Collections.emptySet(),
            key1Bytes,
            key2Bytes,
            true,
            false
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key1Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator);
    }

}
