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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
abstract class AbstractColumnFamilyAccessorTest {

    @Mock
    protected ColumnFamilyHandle offsetsCF;

    @Mock
    protected RocksDBStore.DBAccessor dbAccessor;

    protected AbstractColumnFamilyAccessor accessor;

    abstract AbstractColumnFamilyAccessor createColumnFamilyAccessor();
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
        when(dbAccessor.get(offsetsCF, toBytes("status"))).thenReturn(closedValue);

        // Open the ColumnFamily
        accessor.open(dbAccessor, false);
        verify(dbAccessor).put(eq(offsetsCF), eq(toBytes("status")), eq(openValue));

        // Now close the ColumnFamily
        accessor.close(dbAccessor);
        verify(dbAccessor).put(eq(offsetsCF), eq(toBytes("status")), eq(closedValue));
    }

    @Test
    public void shouldThrowOnOpenAfterAUncleanClose() throws RocksDBException {
        when(dbAccessor.get(offsetsCF, toBytes("status"))).thenReturn(openValue);
        final ProcessorStateException thrown = assertThrowsExactly(ProcessorStateException.class, () -> accessor.open(dbAccessor, false));
        assertEquals("Invalid state during store open. Expected state to be either empty or closed", thrown.getMessage());
    }

    @Test
    public void shouldIgnoreExceptionAfterUncleanClose() throws RocksDBException {
        when(dbAccessor.get(offsetsCF, toBytes("status"))).thenReturn(openValue);
        accessor.open(dbAccessor, true);
        assertTrue(storeOpen.get());
        verify(dbAccessor).put(eq(offsetsCF), eq(toBytes("status")), eq(openValue));
    }

    @Test
    public void shouldCommitOffsets() throws RocksDBException {
        final TopicPartition tp0 = new TopicPartition("testTopic", 0);
        final TopicPartition tp1 = new TopicPartition("testTopic", 1);
        final Map<TopicPartition, Long> changelogOffsets = Map.of(tp0, 10L, tp1, 20L);
        accessor.commit(dbAccessor, changelogOffsets);
        verify(dbAccessor).flush(any(ColumnFamilyHandle[].class));
        verify(dbAccessor).put(eq(offsetsCF), eq(toBytes(tp0.toString())), eq(toBytes(10L)));
        verify(dbAccessor).put(eq(offsetsCF), eq(toBytes(tp1.toString())), eq(toBytes(20L)));
    }

    private byte[] toBytes(final String s) {
        return keySerializer.serialize("", s);
    }
    
    private byte[] toBytes(final long l) {
        return offsetSerializer.serialize("", l);
    }

}