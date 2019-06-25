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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class AbstractByteStoreTest {

    protected abstract <K, V> KeyValueStore<K, V> createKeyValueStore(final ProcessorContext context);

    protected InternalMockProcessorContext context;
    protected KeyValueStore<Bytes, byte[]> store;
    protected KeyValueStoreTestDriver<Bytes, byte[]> driver;

    @Before
    public void before() {
        driver = KeyValueStoreTestDriver.create(Bytes.class, byte[].class);
        context = (InternalMockProcessorContext) driver.context();
        context.setTime(10);
        store = createKeyValueStore(context);
    }

    @After
    public void after() {
        store.close();
        driver.clear();
    }

    static Map<Bytes, byte[]> getContents(final KeyValueIterator<Bytes, byte[]> iter) {
        final HashMap<Bytes, byte[]> result = new HashMap<>();
        while (iter.hasNext()) {
            final KeyValue<Bytes, byte[]> entry = iter.next();
            result.put(entry.key, entry.value);
        }
        return result;
    }

    @Test
    public void doDefaultComparatorPrefixScan() {
        byte[] value = new byte[]{0x00};

        Bytes key = Bytes.wrap(new byte[]{(byte)0xFF});
        Bytes key2 = Bytes.wrap(new byte[]{(byte)0xFF, (byte)0x00});
        Bytes key3 = Bytes.wrap(new byte[]{(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF});
        Bytes key4 = Bytes.wrap(new byte[]{(byte)0x01, (byte)0xFF});
        Bytes key5 = Bytes.wrap(new byte[]{(byte)0x00});

        store.put(key, value);
        store.put(key2, value);
        store.put(key3, value);
        store.put(key4, value);
        store.put(key5, value);

        final Map<Bytes, byte[]> expectedContents = new HashMap<>();
        expectedContents.put(key, value);
        expectedContents.put(key2, value);
        expectedContents.put(key3, value);

        assertEquals(expectedContents, getContents(store.prefixScan(key)));
    }
}

