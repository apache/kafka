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

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.utils.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.RocksIterator;

import java.util.NoSuchElementException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
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
        doNothing().when(rocksIterator).seek(key1Bytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key1Bytes.get())
            .thenReturn(key2Bytes.get())
            .thenReturn(key3Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).next();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(3)).value();
        verify(rocksIterator, times(3)).next();
    }

    @Test
    public void shouldReturnAllKeysInTheRangeReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seekForPrev(key3Bytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key3Bytes.get())
            .thenReturn(key2Bytes.get())
            .thenReturn(key1Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).prev();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(3)).value();
        verify(rocksIterator, times(3)).prev();
    }

    @Test
    public void shouldReturnAllKeysWhenLastKeyIsGreaterThanLargestKeyInStateStoreInForwardDirection() {
        final Bytes toBytes = Bytes.increment(key4Bytes);
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seek(key1Bytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key1Bytes.get())
            .thenReturn(key2Bytes.get())
            .thenReturn(key3Bytes.get())
            .thenReturn(key4Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).next();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(4)).value();
        verify(rocksIterator, times(4)).next();
    }


    @Test
    public void shouldReturnAllKeysWhenLastKeyIsSmallerThanSmallestKeyInStateStoreInReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seekForPrev(key4Bytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key4Bytes.get())
            .thenReturn(key3Bytes.get())
            .thenReturn(key2Bytes.get())
            .thenReturn(key1Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).prev();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(4)).value();
        verify(rocksIterator, times(4)).prev();
    }


    @Test
    public void shouldReturnNoKeysWhenLastKeyIsSmallerThanSmallestKeyInStateStoreForwardDirection() {
        // key range in state store: [c-f]
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seek(key1Bytes.get());
        when(rocksIterator.isValid()).thenReturn(false);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            key1Bytes,
            key2Bytes,
            true,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnNoKeysWhenLastKeyIsLargerThanLargestKeyInStateStoreReverseDirection() {
        // key range in state store: [c-f]
        final String from = "g";
        final String to = "h";
        final  Bytes fromBytes = Bytes.wrap(from.getBytes());
        final  Bytes toBytes = Bytes.wrap(to.getBytes());
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seekForPrev(toBytes.get());
        when(rocksIterator.isValid())
            .thenReturn(false);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            fromBytes,
            toBytes,
            false,
            true
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
    }

    @Test
    public void shouldReturnAllKeysInPartiallyOverlappingRangeInForwardDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seek(key1Bytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key2Bytes.get())
            .thenReturn(key3Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).next();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(2)).value();
        verify(rocksIterator, times(2)).next();
    }

    @Test
    public void shouldReturnAllKeysInPartiallyOverlappingRangeInReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        final String to = "e";
        final Bytes toBytes = Bytes.wrap(to.getBytes());
        doNothing().when(rocksIterator).seekForPrev(toBytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key4Bytes.get())
            .thenReturn(key3Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).prev();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(2)).value();
        verify(rocksIterator, times(2)).prev();
    }

    @Test
    public void shouldReturnTheCurrentKeyOnInvokingPeekNextKeyInForwardDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seek(key1Bytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key2Bytes.get())
            .thenReturn(key3Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).next();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(2)).value();
        verify(rocksIterator, times(2)).next();
    }

    @Test
    public void shouldReturnTheCurrentKeyOnInvokingPeekNextKeyInReverseDirection() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        final Bytes toBytes = Bytes.increment(key4Bytes);
        doNothing().when(rocksIterator).seekForPrev(toBytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rocksIterator.key())
            .thenReturn(key4Bytes.get())
            .thenReturn(key3Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).prev();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
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
        verify(rocksIterator, times(2)).value();
        verify(rocksIterator, times(2)).prev();
    }

    @Test
    public void shouldCloseIterator() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seek(key1Bytes.get());
        doNothing().when(rocksIterator).close();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            key1Bytes,
            key2Bytes,
            true,
            true
        );
        rocksDBRangeIterator.onClose(() -> { });
        rocksDBRangeIterator.close();
        verify(rocksIterator).close();
    }

    @Test
    public void shouldCallCloseCallbackOnClose() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            key1Bytes,
            key2Bytes,
            true,
            true
        );
        final AtomicBoolean callbackCalled = new AtomicBoolean(false);
        rocksDBRangeIterator.onClose(() -> callbackCalled.set(true));
        rocksDBRangeIterator.close();
        assertThat(callbackCalled.get(), is(true));
    }

    @Test
    public void shouldExcludeEndOfRange() {
        final RocksIterator rocksIterator = mock(RocksIterator.class);
        doNothing().when(rocksIterator).seek(key1Bytes.get());
        when(rocksIterator.isValid())
            .thenReturn(true);
        when(rocksIterator.key())
            .thenReturn(key1Bytes.get())
            .thenReturn(key2Bytes.get());
        when(rocksIterator.value()).thenReturn(valueBytes);
        doNothing().when(rocksIterator).next();
        final RocksDBRangeIterator rocksDBRangeIterator = new RocksDBRangeIterator(
            storeName,
            rocksIterator,
            key1Bytes,
            key2Bytes,
            true,
            false
        );
        assertThat(rocksDBRangeIterator.hasNext(), is(true));
        assertThat(rocksDBRangeIterator.next().key, is(key1Bytes));
        assertThat(rocksDBRangeIterator.hasNext(), is(false));
        verify(rocksIterator, times(2)).value();
        verify(rocksIterator, times(2)).next();
    }

}
