/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.KeyValue;
import org.junit.Test;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;

public class MemoryLRUCacheBytesTest  {

    @Test
    public void basicPutGet() {
        List<KeyValue<String, String>> toInsert = Arrays.asList(
            new KeyValue<>("K1", "V1"),
            new KeyValue<>("K2", "V2"),
            new KeyValue<>("K3", "V3"),
            new KeyValue<>("K4", "V4"),
            new KeyValue<>("K5", "V5"));
        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                toInsert.size() * entrySize(toInsert.get(0)));

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(key, new MemoryLRUCacheBytesEntry(key, value, true, 1L, 1L, 1, ""));
        }

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            MemoryLRUCacheBytesEntry entry = cache.get(key);
            assertEquals(entry.isDirty, true);
            assertEquals(new String(entry.value), toInsert.get(i).value);
        }
    }

    @Test
    public void headTail() {
        List<KeyValue<String, String>> toInsert = Arrays.asList(
            new KeyValue<>("K1", "V1"),
            new KeyValue<>("K2", "V2"),
            new KeyValue<>("K3", "V3"),
            new KeyValue<>("K4", "V4"),
            new KeyValue<>("K5", "V5"));
        final KeyValue<String, String> kv = toInsert.get(0);
        final int length = entrySize(kv);

        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                toInsert.size() * length);

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(key, new MemoryLRUCacheBytesEntry(key, value, true, 1, 1, 1, ""));
            MemoryLRUCacheBytesEntry head = cache.head().entry();
            MemoryLRUCacheBytesEntry tail = cache.tail().entry();
            assertEquals(new String(head.value), toInsert.get(i).value);
            assertEquals(new String(tail.value), toInsert.get(0).value);
        }
    }

    private int entrySize(final KeyValue<String, String> kv) {
        return kv.key.getBytes().length +
                    kv.value.getBytes().length +
                    1 + // isDirty
                    8 + // timestamp
                    8 + // offset
                    4;
    }

    @Test
    public void evict() {
        final List<KeyValue<String, String>> received = new ArrayList<>();
        List<KeyValue<String, String>> expected = Arrays.asList(
            new KeyValue<>("K1", "V1"));

        List<KeyValue<String, String>> toInsert = Arrays.asList(
            new KeyValue<>("K1", "V1"),
            new KeyValue<>("K2", "V2"),
            new KeyValue<>("K3", "V3"),
            new KeyValue<>("K4", "V4"),
            new KeyValue<>("K5", "V5"));
        MemoryLRUCacheBytes cache = new MemoryLRUCacheBytes(
                entrySize(toInsert.get(0)));
        cache.addEldestRemovedListener(new MemoryLRUCacheBytes.EldestEntryRemovalListener<byte[],
            MemoryLRUCacheBytesEntry>() {
            @Override
            public void apply(byte[] key, MemoryLRUCacheBytesEntry value) {

                received.add(new KeyValue<>(new String(key),
                    new String(((MemoryLRUCacheBytesEntry) value).value)));
            }
        });

        for (int i = 0; i < toInsert.size(); i++) {
            byte[] key = toInsert.get(i).key.getBytes();
            byte[] value = toInsert.get(i).value.getBytes();
            cache.put(key, new MemoryLRUCacheBytesEntry(key, value, true, 1, 1, 1, ""));
        }

        for (int i = 0; i < expected.size(); i++) {
            KeyValue<String, String> expectedRecord = expected.get(i);
            KeyValue<String, String> actualRecord = received.get(i);
            assertEquals(expectedRecord, actualRecord);
        }
    }
}