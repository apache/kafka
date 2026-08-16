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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchInterface;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DualColumnFamilyAccessorTest extends AbstractColumnFamilyAccessorTest {

    @Mock
    private ColumnFamilyHandle oldCF;

    @Mock
    private ColumnFamilyHandle newCF;

    private Function<byte[], byte[]> valueConverter;

    private static final String STORE_NAME = "test-store";
    private static final byte[] KEY = "key".getBytes();
    private static final byte[] OLD_VALUE = "old-value".getBytes();
    private static final byte[] NEW_VALUE = "new-value".getBytes();

    @Override
    AbstractColumnFamilyAccessor createColumnFamilyAccessor() {
        // Create a real Position object
        final Position position = Position.emptyPosition();

        // Create a mock store with real position field
        final RocksDBStore store = mock(RocksDBStore.class);
        store.position = position;
        store.context = mock(StateStoreContext.class);
        lenient().when(store.name()).thenReturn(STORE_NAME);

        // Value converter that adds a prefix to indicate conversion
        valueConverter = oldValue -> {
            if (oldValue == null) {
                return null;
            }
            return ByteBuffer.allocate(oldValue.length + 10).put("converted:".getBytes()).put(oldValue).array();
        };

        return new DualColumnFamilyAccessor(offsetsCF, oldCF, newCF, valueConverter, store, storeOpen);
    }

    @Override
    AbstractColumnFamilyAccessor createTransactionalColumnFamilyAccessor() {
        final RocksDBStore store = mock(RocksDBStore.class);
        store.position = Position.emptyPosition();
        store.context = mock(StateStoreContext.class);
        store.isTransactional = true;
        lenient().when(store.name()).thenReturn(STORE_NAME);
        if (valueConverter == null) {
            valueConverter = oldValue -> oldValue == null ? null
                    : ByteBuffer.allocate(oldValue.length + 10).put("converted:".getBytes()).put(oldValue).array();
        }
        return new DualColumnFamilyAccessor(offsetsCF, oldCF, newCF, valueConverter, store, storeOpen);
    }

    @Test
    public void shouldPutValueToNewColumnFamilyAndDeleteFromOld() throws RocksDBException {
        accessor.put(dbAccessor, KEY, NEW_VALUE);

        verify(dbAccessor).delete(oldCF, KEY);
        verify(dbAccessor).put(newCF, KEY, NEW_VALUE);
        verify(dbAccessor, never()).delete(newCF, KEY);
    }

    @Test
    public void shouldDeleteFromBothColumnFamiliesWhenValueIsNull() throws RocksDBException {
        accessor.put(dbAccessor, KEY, null);

        verify(dbAccessor).delete(oldCF, KEY);
        verify(dbAccessor).delete(newCF, KEY);
        verify(dbAccessor, never()).put(any(), any(), any());
    }

    @Test
    public void shouldThrowProcessorStateExceptionWhenPutFailsOnDeleteColumnFamily() throws RocksDBException {
        doThrow(new RocksDBException("Delete failed")).when(dbAccessor).delete(oldCF, KEY);

        final ProcessorStateException exception = assertThrows(ProcessorStateException.class, () -> accessor.put(dbAccessor, KEY, NEW_VALUE));

        assertEquals("Error while removing key from store " + STORE_NAME, exception.getMessage());
    }

    @Test
    public void shouldThrowProcessorStateExceptionWhenPutFailsOnNewColumnFamily() throws RocksDBException {
        doThrow(new RocksDBException("Put failed")).when(dbAccessor).put(newCF, KEY, NEW_VALUE);

        final ProcessorStateException exception = assertThrows(ProcessorStateException.class, () -> accessor.put(dbAccessor, KEY, NEW_VALUE));

        assertEquals("Error while putting key/value into store " + STORE_NAME, exception.getMessage());
    }

    @Test
    public void shouldThrowProcessorStateExceptionWhenDeleteFailsOnOldColumnFamilyWithNullValue() throws RocksDBException {
        doThrow(new RocksDBException("Delete failed")).when(dbAccessor).delete(eq(oldCF), any(byte[].class));

        final ProcessorStateException exception = assertThrows(ProcessorStateException.class, () -> accessor.put(dbAccessor, KEY, null));

        assertEquals("Error while removing key from store " + STORE_NAME, exception.getMessage());
    }

    @Test
    public void shouldThrowProcessorStateExceptionWhenDeleteFailsOnNewColumnFamilyWithNullValue() throws RocksDBException {
        lenient().doNothing().when(dbAccessor).delete(eq(oldCF), any(byte[].class));
        doThrow(new RocksDBException("Delete failed")).when(dbAccessor).delete(eq(newCF), any(byte[].class));

        final ProcessorStateException exception = assertThrows(ProcessorStateException.class, () -> accessor.put(dbAccessor, KEY, null));

        assertEquals("Error while removing key from store " + STORE_NAME, exception.getMessage());
        verify(dbAccessor).delete(oldCF, KEY); // Should have tried to delete from old first
    }

    @Test
    public void shouldGetValueFromNewColumnFamily() throws RocksDBException {
        when(dbAccessor.get(newCF, KEY)).thenReturn(NEW_VALUE);

        final byte[] result = accessor.get(dbAccessor, KEY);

        assertArrayEquals(NEW_VALUE, result);
        verify(dbAccessor).get(newCF, KEY);
        verify(dbAccessor, never()).get(oldCF, KEY);
    }

    @Test
    public void shouldGetValueFromOldColumnFamilyAndConvert() throws RocksDBException {
        when(dbAccessor.get(newCF, KEY)).thenReturn(null);
        when(dbAccessor.get(oldCF, KEY)).thenReturn(OLD_VALUE);

        final byte[] result = accessor.get(dbAccessor, KEY);

        assertNotNull(result);
        verify(dbAccessor).get(newCF, KEY);
        verify(dbAccessor).get(oldCF, KEY);
        // Should be converted value
        assertArrayEquals(valueConverter.apply(OLD_VALUE), result);
        // Should migrate the value to new CF
        verify(dbAccessor).delete(oldCF, KEY);
        verify(dbAccessor).put(eq(newCF), eq(KEY), any());
    }

    @Test
    public void shouldReturnNullWhenKeyNotFoundInEitherColumnFamily() throws RocksDBException {
        when(dbAccessor.get(newCF, KEY)).thenReturn(null);
        when(dbAccessor.get(oldCF, KEY)).thenReturn(null);

        final byte[] result = accessor.get(dbAccessor, KEY);

        assertNull(result);
        verify(dbAccessor).get(newCF, KEY);
        verify(dbAccessor).get(oldCF, KEY);
    }

    @Test
    public void shouldGetValueWithReadOptions() throws RocksDBException {
        final ReadOptions readOptions = mock(ReadOptions.class);
        when(dbAccessor.get(newCF, readOptions, KEY)).thenReturn(NEW_VALUE);

        final byte[] result = accessor.get(dbAccessor, KEY, readOptions);

        assertArrayEquals(NEW_VALUE, result);
        verify(dbAccessor).get(newCF, readOptions, KEY);
        verify(dbAccessor, never()).get(eq(oldCF), eq(readOptions), eq(KEY));
    }

    @Test
    public void shouldGetFromOldColumnFamilyWithReadOptionsAndConvert() throws RocksDBException {
        final ReadOptions readOptions = mock(ReadOptions.class);
        when(dbAccessor.get(newCF, readOptions, KEY)).thenReturn(null);
        when(dbAccessor.get(oldCF, readOptions, KEY)).thenReturn(OLD_VALUE);

        final byte[] result = accessor.get(dbAccessor, KEY, readOptions);

        assertNotNull(result);
        verify(dbAccessor).get(newCF, readOptions, KEY);
        verify(dbAccessor).get(oldCF, readOptions, KEY);
        // Should be converted value
        assertArrayEquals(valueConverter.apply(OLD_VALUE), result);
        // Should migrate the value to new CF
        verify(dbAccessor).delete(oldCF, KEY);
        verify(dbAccessor).put(eq(newCF), eq(KEY), any());
    }

    @Test
    public void shouldGetOnlyFromOldColumnFamilyAndConvertWithoutMigration() throws RocksDBException {
        when(dbAccessor.get(newCF, KEY)).thenReturn(null);
        when(dbAccessor.get(oldCF, KEY)).thenReturn(OLD_VALUE);

        final byte[] result = accessor.getOnly(dbAccessor, KEY);

        assertNotNull(result);
        verify(dbAccessor).get(newCF, KEY);
        verify(dbAccessor).get(oldCF, KEY);
        // Should be converted value
        assertArrayEquals(valueConverter.apply(OLD_VALUE), result);
        // getOnly should NOT migrate (no put/delete calls)
        verify(dbAccessor, never()).put(any(), any(), any());
        verify(dbAccessor, never()).delete(any(), any());
    }

    @Test
    public void shouldGetOnlyReturnNullWhenNotFound() throws RocksDBException {
        when(dbAccessor.get(newCF, KEY)).thenReturn(null);
        when(dbAccessor.get(oldCF, KEY)).thenReturn(null);

        final byte[] result = accessor.getOnly(dbAccessor, KEY);

        assertNull(result);
    }

    @Test
    public void shouldPrepareBatchWithMultipleEntries() throws RocksDBException {
        final WriteBatchInterface batch = mock(WriteBatchInterface.class);
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(new Bytes("key1".getBytes()), "value1".getBytes()));
        entries.add(new KeyValue<>(new Bytes("key2".getBytes()), "value2".getBytes()));
        entries.add(new KeyValue<>(new Bytes("key3".getBytes()), null)); // Delete

        accessor.prepareBatch(entries, batch);

        verify(batch).delete(oldCF, "key1".getBytes());
        verify(batch).put(newCF, "key1".getBytes(), "value1".getBytes());
        verify(batch).delete(oldCF, "key2".getBytes());
        verify(batch).put(newCF, "key2".getBytes(), "value2".getBytes());
        verify(batch).delete(oldCF, "key3".getBytes());
        verify(batch).delete(newCF, "key3".getBytes());
    }

    @Test
    public void shouldThrowNPEWhenBatchEntryHasNullKey() {
        final WriteBatchInterface batch = mock(WriteBatchInterface.class);
        final List<KeyValue<Bytes, byte[]>> entries = new ArrayList<>();
        entries.add(new KeyValue<>(null, "value".getBytes()));

        assertThrows(NullPointerException.class, () -> accessor.prepareBatch(entries, batch));
    }

    @Test
    public void shouldAddToBatchDeletesFromOldAndPutsToNew() throws RocksDBException {
        final WriteBatchInterface batch = mock(WriteBatchInterface.class);

        accessor.addToBatch(KEY, NEW_VALUE, batch);

        verify(batch).delete(oldCF, KEY);
        verify(batch).put(newCF, KEY, NEW_VALUE);
    }

    @Test
    public void shouldAddToBatchDeletesFromBothWhenValueIsNull() throws RocksDBException {
        final WriteBatchInterface batch = mock(WriteBatchInterface.class);

        accessor.addToBatch(KEY, null, batch);

        verify(batch).delete(oldCF, KEY);
        verify(batch).delete(newCF, KEY);
        verify(batch, never()).put(any(ColumnFamilyHandle.class), any(byte[].class), any(byte[].class));
    }

    @Test
    public void shouldDeleteRangeFromBothColumnFamilies() throws RocksDBException {
        final byte[] from = "a".getBytes();
        final byte[] to = "z".getBytes();

        accessor.deleteRange(dbAccessor, from, to);

        verify(dbAccessor).deleteRange(oldCF, from, to);
        verify(dbAccessor).deleteRange(newCF, from, to);
    }

    @Test
    public void shouldThrowProcessorStateExceptionWhenDeleteRangeFailsOnOldColumnFamily() throws RocksDBException {
        final byte[] from = "a".getBytes();
        final byte[] to = "z".getBytes();

        doThrow(new RocksDBException("Delete range failed")).when(dbAccessor).deleteRange(eq(oldCF), any(byte[].class), any(byte[].class));

        final ProcessorStateException exception = assertThrows(ProcessorStateException.class, () -> accessor.deleteRange(dbAccessor, from, to));

        assertEquals("Error while removing key from store " + STORE_NAME, exception.getMessage());
    }

    @Test
    public void shouldThrowProcessorStateExceptionWhenDeleteRangeFailsOnNewColumnFamily() throws RocksDBException {
        final byte[] from = "a".getBytes();
        final byte[] to = "z".getBytes();

        lenient().doNothing().when(dbAccessor).deleteRange(eq(oldCF), any(byte[].class), any(byte[].class));
        doThrow(new RocksDBException("Delete range failed")).when(dbAccessor).deleteRange(eq(newCF), any(byte[].class), any(byte[].class));

        final ProcessorStateException exception = assertThrows(ProcessorStateException.class, () -> accessor.deleteRange(dbAccessor, from, to));

        assertEquals("Error while removing key from store " + STORE_NAME, exception.getMessage());
        verify(dbAccessor).deleteRange(oldCF, from, to);
    }

    @Test
    public void shouldReturnSumOfEntriesFromBothColumnFamilies() throws RocksDBException {
        when(dbAccessor.approximateNumEntries(oldCF)).thenReturn(100L);
        when(dbAccessor.approximateNumEntries(newCF)).thenReturn(50L);

        final long result = accessor.approximateNumEntries(dbAccessor);

        assertEquals(150L, result);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateRangeIterator() {
        final ManagedKeyValueIterator<Bytes, byte[]> iterNew = mock(ManagedKeyValueIterator.class);
        final ManagedKeyValueIterator<Bytes, byte[]> iterOld = mock(ManagedKeyValueIterator.class);
        final Bytes from = new Bytes("a".getBytes());
        final Bytes to = new Bytes("z".getBytes());
        when(dbAccessor.range(newCF, STORE_NAME, from, to, true, true)).thenReturn(iterNew);
        when(dbAccessor.range(oldCF, STORE_NAME, from, to, true, true)).thenReturn(iterOld);

        final ManagedKeyValueIterator<Bytes, byte[]> iterator = accessor.range(dbAccessor, from, to, true);

        assertNotNull(iterator);
        verify(dbAccessor).range(newCF, STORE_NAME, from, to, true, true);
        verify(dbAccessor).range(oldCF, STORE_NAME, from, to, true, true);
        verify(dbAccessor, never()).newIterator(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateAllIteratorForward() {
        final ManagedKeyValueIterator<Bytes, byte[]> iterNew = mock(ManagedKeyValueIterator.class);
        final ManagedKeyValueIterator<Bytes, byte[]> iterOld = mock(ManagedKeyValueIterator.class);
        when(dbAccessor.all(newCF, STORE_NAME, true)).thenReturn(iterNew);
        when(dbAccessor.all(oldCF, STORE_NAME, true)).thenReturn(iterOld);

        final ManagedKeyValueIterator<Bytes, byte[]> iterator = accessor.all(dbAccessor, true);

        assertNotNull(iterator);
        verify(dbAccessor).all(newCF, STORE_NAME, true);
        verify(dbAccessor).all(oldCF, STORE_NAME, true);
        verify(dbAccessor, never()).newIterator(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreateAllIteratorReverse() {
        final ManagedKeyValueIterator<Bytes, byte[]> iterNew = mock(ManagedKeyValueIterator.class);
        final ManagedKeyValueIterator<Bytes, byte[]> iterOld = mock(ManagedKeyValueIterator.class);
        when(dbAccessor.all(newCF, STORE_NAME, false)).thenReturn(iterNew);
        when(dbAccessor.all(oldCF, STORE_NAME, false)).thenReturn(iterOld);

        final ManagedKeyValueIterator<Bytes, byte[]> iterator = accessor.all(dbAccessor, false);

        assertNotNull(iterator);
        verify(dbAccessor).all(newCF, STORE_NAME, false);
        verify(dbAccessor).all(oldCF, STORE_NAME, false);
        verify(dbAccessor, never()).newIterator(any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCreatePrefixScanIterator() {
        final ManagedKeyValueIterator<Bytes, byte[]> iterNew = mock(ManagedKeyValueIterator.class);
        final ManagedKeyValueIterator<Bytes, byte[]> iterOld = mock(ManagedKeyValueIterator.class);
        final Bytes prefix = new Bytes("prefix".getBytes());
        when(dbAccessor.prefixScan(eq(newCF), eq(STORE_NAME), eq(prefix), any())).thenReturn(iterNew);
        when(dbAccessor.prefixScan(eq(oldCF), eq(STORE_NAME), eq(prefix), any())).thenReturn(iterOld);

        final ManagedKeyValueIterator<Bytes, byte[]> iterator = accessor.prefixScan(dbAccessor, prefix);

        assertNotNull(iterator);
        verify(dbAccessor).prefixScan(eq(newCF), eq(STORE_NAME), eq(prefix), any());
        verify(dbAccessor).prefixScan(eq(oldCF), eq(STORE_NAME), eq(prefix), any());
        verify(dbAccessor, never()).newIterator(any());
    }

    @Test
    public void shouldMergeEntriesFromBothColumnFamiliesInForwardOrder() throws RocksDBException {
        final InMemoryRocksDBAccessor inMemory = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        inMemory.put(oldCF, "a".getBytes(), "old-a".getBytes());
        inMemory.put(oldCF, "c".getBytes(), "old-c".getBytes());
        inMemory.put(newCF, "b".getBytes(), "new-b".getBytes());
        inMemory.put(newCF, "d".getBytes(), "new-d".getBytes());

        final ManagedKeyValueIterator<Bytes, byte[]> it = accessor.all(inMemory, true);
        it.onClose(() -> { });

        assertTrue(it.hasNext());
        final KeyValue<Bytes, byte[]> kv0 = it.next();
        assertArrayEquals("a".getBytes(), kv0.key.get());
        assertTrue(new String(kv0.value).contains("old-a"), "expected converted old-a value");

        assertTrue(it.hasNext());
        final KeyValue<Bytes, byte[]> kv1 = it.next();
        assertArrayEquals("b".getBytes(), kv1.key.get());
        assertArrayEquals("new-b".getBytes(), kv1.value);

        assertTrue(it.hasNext());
        final KeyValue<Bytes, byte[]> kv2 = it.next();
        assertArrayEquals("c".getBytes(), kv2.key.get());
        assertTrue(new String(kv2.value).contains("old-c"), "expected converted old-c value");

        assertTrue(it.hasNext());
        final KeyValue<Bytes, byte[]> kv3 = it.next();
        assertArrayEquals("d".getBytes(), kv3.key.get());
        assertArrayEquals("new-d".getBytes(), kv3.value);

        assertFalse(it.hasNext());
    }

    @Test
    public void shouldMergeEntriesFromBothColumnFamiliesInReverseOrder() throws RocksDBException {
        final InMemoryRocksDBAccessor inMemory = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        inMemory.put(oldCF, "a".getBytes(), "old-a".getBytes());
        inMemory.put(oldCF, "c".getBytes(), "old-c".getBytes());
        inMemory.put(newCF, "b".getBytes(), "new-b".getBytes());
        inMemory.put(newCF, "d".getBytes(), "new-d".getBytes());

        final ManagedKeyValueIterator<Bytes, byte[]> it = accessor.all(inMemory, false);
        it.onClose(() -> { });

        final List<Bytes> keys = new ArrayList<>();
        while (it.hasNext()) {
            keys.add(it.next().key);
        }
        assertEquals(4, keys.size());
        assertTrue(new String(keys.get(0).get()).compareTo(new String(keys.get(1).get())) > 0,
                "expected descending order");
        assertTrue(new String(keys.get(1).get()).compareTo(new String(keys.get(2).get())) > 0,
                "expected descending order");
        assertTrue(new String(keys.get(2).get()).compareTo(new String(keys.get(3).get())) > 0,
                "expected descending order");
    }

    @Test
    public void shouldRespectRangeBoundsAcrossBothColumnFamilies() throws RocksDBException {
        final InMemoryRocksDBAccessor inMemory = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        inMemory.put(oldCF, "a".getBytes(), "old-a".getBytes());
        inMemory.put(oldCF, "c".getBytes(), "old-c".getBytes());
        inMemory.put(newCF, "b".getBytes(), "new-b".getBytes());
        inMemory.put(newCF, "d".getBytes(), "new-d".getBytes());

        final ManagedKeyValueIterator<Bytes, byte[]> it =
                accessor.range(inMemory, Bytes.wrap("b".getBytes()), Bytes.wrap("c".getBytes()), true);
        it.onClose(() -> { });

        final List<byte[]> keys = new ArrayList<>();
        while (it.hasNext()) {
            keys.add(it.next().key.get());
        }
        assertEquals(2, keys.size());
        assertArrayEquals("b".getBytes(), keys.get(0));
        assertArrayEquals("c".getBytes(), keys.get(1));
    }

    @Test
    public void shouldRespectPrefixScanAcrossBothColumnFamilies() throws RocksDBException {
        final InMemoryRocksDBAccessor inMemory = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        inMemory.put(oldCF, "foo:1".getBytes(), "old-1".getBytes());
        inMemory.put(newCF, "foo:2".getBytes(), "new-2".getBytes());
        inMemory.put(newCF, "bar:1".getBytes(), "new-bar".getBytes());

        final ManagedKeyValueIterator<Bytes, byte[]> it =
                accessor.prefixScan(inMemory, Bytes.wrap("foo:".getBytes()));
        it.onClose(() -> { });

        final List<byte[]> keys = new ArrayList<>();
        while (it.hasNext()) {
            keys.add(it.next().key.get());
        }
        assertEquals(2, keys.size());
        assertArrayEquals("foo:1".getBytes(), keys.get(0));
        assertArrayEquals("foo:2".getBytes(), keys.get(1));
    }

    @Test
    public void shouldPreferNewFormatValueWhenSameKeyExistsInBothColumnFamilies() throws RocksDBException {
        final InMemoryRocksDBAccessor inMemory = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        inMemory.put(oldCF, "x".getBytes(), "old-x".getBytes());
        inMemory.put(newCF, "x".getBytes(), "new-x".getBytes());

        final ManagedKeyValueIterator<Bytes, byte[]> it = accessor.all(inMemory, true);
        it.onClose(() -> { });

        assertTrue(it.hasNext());
        final KeyValue<Bytes, byte[]> kv = it.next();
        assertArrayEquals("x".getBytes(), kv.key.get());
        assertArrayEquals("new-x".getBytes(), kv.value,
                "new-format value should win over old-format value for the same key");
        assertFalse(it.hasNext(), "same key should appear exactly once");
    }

    @Test
    public void shouldCloseBothColumnFamilies() throws RocksDBException {
        accessor.close(dbAccessor);

        verify(oldCF).close();
        verify(newCF).close();
    }
}