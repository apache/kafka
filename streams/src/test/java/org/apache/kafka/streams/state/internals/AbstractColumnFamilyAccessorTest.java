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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.query.Position;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
abstract class AbstractColumnFamilyAccessorTest {

    @Mock
    protected ColumnFamilyHandle offsetsCF;

    @Mock
    protected RocksDBStore.DBAccessor dbAccessor;

    protected AbstractColumnFamilyAccessor accessor;

    abstract AbstractColumnFamilyAccessor createColumnFamilyAccessor();

    abstract AbstractColumnFamilyAccessor createTransactionalColumnFamilyAccessor();
    private final LongSerializer offsetSerializer = new LongSerializer();
    private final StringSerializer keySerializer = new StringSerializer();
    private final byte[] openValue = toBytes(1L);
    private final byte[] closedValue = toBytes(0L);
    protected final AtomicBoolean storeOpen = new AtomicBoolean(false);


    @BeforeEach
    public void setUp() {
        accessor = createColumnFamilyAccessor();
    }

    @Test
    public void shouldOpenClean() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        // Open the ColumnFamily
        accessor.open(dbAccessor, false);
        assertArrayEquals(openValue, dbAccessor.get(offsetsCF, toBytes("status")));

        // Now close the ColumnFamily
        accessor.close(dbAccessor);
        assertArrayEquals(closedValue, dbAccessor.get(offsetsCF, toBytes("status")));

        // Open clean again
        accessor.open(dbAccessor, false);
        assertArrayEquals(openValue, dbAccessor.get(offsetsCF, toBytes("status")));
    }

    @Test
    public void shouldThrowOnOpenAfterAUncleanClose() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        // First, open clean
        accessor.open(dbAccessor, false);

        // Try to open again, with ignoreUncleanClose=false, which should throw since the store is already open
        final ProcessorStateException thrown = assertThrowsExactly(ProcessorStateException.class, () -> accessor.open(dbAccessor, false));
        assertEquals("Invalid state during store open. Expected state to be either empty or closed", thrown.getMessage());
    }

    @Test
    public void shouldIgnoreExceptionAfterUncleanClose() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        // First, open clean
        accessor.open(dbAccessor, false);
        // Now reopen in an invalid state
        accessor.open(dbAccessor, true);
        assertTrue(storeOpen.get());
        assertArrayEquals(openValue, dbAccessor.get(offsetsCF, toBytes("status")));
    }

    @Test
    public void shouldCommitOffsets() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        final TopicPartition tp0 = new TopicPartition("testTopic", 0);
        final TopicPartition tp1 = new TopicPartition("testTopic", 1);
        final Map<TopicPartition, Long> changelogOffsets = Map.of(tp0, 10L, tp1, 20L);
        accessor.commit(dbAccessor, Position.emptyPosition(), changelogOffsets);
        assertEquals(10L, accessor.getCommittedOffset(dbAccessor, tp0));
        assertEquals(20L, accessor.getCommittedOffset(dbAccessor, tp1));
    }

    @Test
    public void shouldCommitPosition() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        final String topic = "testTopic";
        final TopicPartition tp0 = new TopicPartition(topic, 0);
        final TopicPartition tp1 = new TopicPartition(topic, 1);
        final Position positionToStore = Position.fromMap(mkMap(mkEntry(topic, mkMap(mkEntry(tp0.partition(), 10L), mkEntry(tp1.partition(), 20L)))));
        accessor.commit(dbAccessor, positionToStore, Map.of(tp0, 0L));
        assertEquals(positionToStore, PositionSerde.deserialize(ByteBuffer.wrap(dbAccessor.get(offsetsCF, toBytes("position")))));
    }

    @Test
    public void shouldCommitStagedWritesWhenCommittingOffsets() throws RocksDBException {
        final TopicPartition tp0 = new TopicPartition("testTopic", 0);
        accessor.commit(dbAccessor, Position.emptyPosition(), Map.of(tp0, 10L));
        verify(dbAccessor).commitStagedWrites();
    }

    @Test
    public void shouldWipeCommittedOffsetsOnEmptyCommit() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        final TopicPartition tp0 = new TopicPartition("testTopic", 0);
        final TopicPartition tp1 = new TopicPartition("testTopic", 1);
        accessor.commit(dbAccessor, Position.emptyPosition(), Map.of(tp0, 10L, tp1, 20L));
        assertEquals(10L, accessor.getCommittedOffset(dbAccessor, tp0));
        assertEquals(20L, accessor.getCommittedOffset(dbAccessor, tp1));
        accessor.commit(dbAccessor, Position.emptyPosition(), Map.of());
        assertNull(accessor.getCommittedOffset(dbAccessor, tp0));
        assertNull(accessor.getCommittedOffset(dbAccessor, tp1));
    }

    @Test
    public void shouldSkipPersistingStateOnCloseWhenStoreIsAlreadyClosed() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        // Open and close cleanly
        accessor.open(dbAccessor, false);
        accessor.close(dbAccessor);
        assertArrayEquals(closedValue, dbAccessor.get(offsetsCF, toBytes("status")));

        // Simulate unclean shutdown: overwrite status to open without going through accessor.open()
        dbAccessor.put(offsetsCF, toBytes("status"), openValue);
        assertThrowsExactly(ProcessorStateException.class, () -> accessor.open(dbAccessor, false));

        dbAccessor = spy(dbAccessor);
        accessor.close(dbAccessor);
        verify(dbAccessor, never()).put(any(), any(), any());
    }

    @Test
    public void shouldNotWriteOpenMarkerForTransactionalStore() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        final AbstractColumnFamilyAccessor txnAccessor = createTransactionalColumnFamilyAccessor();
        txnAccessor.open(dbAccessor, false);
        assertNull(dbAccessor.get(offsetsCF, toBytes("status")));
    }

    @Test
    public void shouldNotWriteClosedMarkerForTransactionalStore() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        final AbstractColumnFamilyAccessor txnAccessor = createTransactionalColumnFamilyAccessor();
        txnAccessor.open(dbAccessor, false);
        txnAccessor.close(dbAccessor);
        assertNull(dbAccessor.get(offsetsCF, toBytes("status")));
    }

    @Test
    public void shouldDetectStaleOpenMarkerFromPriorNonTransactionalCrash() throws RocksDBException {
        dbAccessor = new InMemoryRocksDBAccessor(mock(RocksDB.class));
        // Simulate a prior non-transactional crash: OPEN marker left on disk
        dbAccessor.put(offsetsCF, toBytes("status"), openValue);

        final AbstractColumnFamilyAccessor txnAccessor = createTransactionalColumnFamilyAccessor();
        final ProcessorStateException thrown = assertThrowsExactly(
                ProcessorStateException.class, () -> txnAccessor.open(dbAccessor, false));
        assertEquals("Invalid state during store open. Expected state to be either empty or closed", thrown.getMessage());
        // Marker is unchanged — transactional accessor never writes status
        assertArrayEquals(openValue, dbAccessor.get(offsetsCF, toBytes("status")));
    }

    private byte[] toBytes(final String s) {
        return keySerializer.serialize("", s);
    }

    private byte[] toBytes(final long l) {
        return offsetSerializer.serialize("", l);
    }

}