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

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.Test;

import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RocksDbIndexedTimeOrderedWindowBytesStoreSupplierTest {

    @Test
    public void shouldThrowIfStoreNameIsNull() {
        final Exception e = assertThrows(NullPointerException.class, () -> RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(null, ZERO, ZERO, false, false));
        assertEquals("name cannot be null", e.getMessage());
    }

    @Test
    public void shouldThrowIfRetentionPeriodIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create("anyName", ofMillis(-1L), ZERO, false, false));
        assertEquals("retentionPeriod cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfWindowSizeIsNegative() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create("anyName", ofMillis(0L), ofMillis(-1L), false, false));
        assertEquals("windowSize cannot be negative", e.getMessage());
    }

    @Test
    public void shouldThrowIfWindowSizeIsLargerThanRetention() {
        final Exception e = assertThrows(IllegalArgumentException.class, () -> RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create("anyName", ofMillis(1L), ofMillis(2L), false, false));
        assertEquals("The retention period of the window store anyName must be no smaller than its window size. Got size=[2], retention=[1]", e.getMessage());
    }

    @Test
    public void shouldCreateRocksDbTimeOrderedWindowStoreWithIndex() {
        final WindowStore store = RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create("store", ofMillis(1L), ofMillis(1L), false, true).get();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(RocksDBTimeOrderedWindowStore.class));
        assertThat(wrapped, instanceOf(RocksDBTimeOrderedWindowSegmentedBytesStore.class));
        assertTrue(((RocksDBTimeOrderedWindowSegmentedBytesStore) wrapped).hasIndex());
    }

    @Test
    public void shouldCreateRocksDbTimeOrderedWindowStoreWithoutIndex() {
        final WindowStore store = RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create("store", ofMillis(1L), ofMillis(1L), false, false).get();
        final StateStore wrapped = ((WrappedStateStore) store).wrapped();
        assertThat(store, instanceOf(RocksDBTimeOrderedWindowStore.class));
        assertThat(wrapped, instanceOf(RocksDBTimeOrderedWindowSegmentedBytesStore.class));
        assertFalse(((RocksDBTimeOrderedWindowSegmentedBytesStore) wrapped).hasIndex());
    }
}
