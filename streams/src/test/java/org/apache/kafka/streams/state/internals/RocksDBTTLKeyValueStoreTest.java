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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestCondition;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class RocksDBTTLKeyValueStoreTest extends RocksDBKeyValueStoreTest {

    final static private int TTL_IN_SECONDS = 1;
    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext context) {
        final StoreBuilder storeBuilder = Stores.keyValueTtlStoreBuilder(
                Stores.persistentKeyValueTtlStore("my-store", TTL_IN_SECONDS),
                (Serde<K>) context.keySerde(),
                (Serde<V>) context.valueSerde());

        final StateStore store = storeBuilder.build();
        store.init(context, store);
        return (KeyValueStore<K, V>) store;
    }

    @Test
    public void shouldDeleteRecordsWhenTTLIsReached() throws InterruptedException {
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        final KeyValueIterator<Integer, String> range = store.all();
        assertEquals("hi", range.next().value);
        assertEquals("goodbye", range.next().value);
        assertFalse(range.hasNext());

        TestUtils.waitForCondition(new TestCondition() {
            @Override
            public boolean conditionMet() {
                compactDatabaseToTriggerDeletion();
                return !store.all().hasNext();
            }
        }, TTL_IN_SECONDS * 1000 * 2, "Records must expire when TTL is set.");
    }

    /** With a TtlDB, values are only expunged during compaction once they've expired
     *  See https://github.com/facebook/rocksdb/wiki/Time-to-Live for more details
     */
    private void compactDatabaseToTriggerDeletion() {
        //We're casting repeatedly in order to get a handle on the underlying TTL RocksDB,
        //so that we can trigger the compaction by hand
        final TtlDB ttlDB = (TtlDB) ((RocksDBTTLStore) ((ChangeLoggingKeyValueBytesStore) ((MeteredKeyValueStore) store).wrapped()).wrapped()).db;
        try {
            ttlDB.compactRange();
        } catch (final RocksDBException n) {
            fail("Native RocksDBException exception thrown: " + n);
        }
    }

}
