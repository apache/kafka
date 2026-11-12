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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.test.InternalMockProcessorContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MemoryLRUCacheTest {

    private MemoryLRUCache cache;
    private InternalMockProcessorContext<?, ?> context;
    private final AtomicInteger evictions = new AtomicInteger();

    @BeforeEach
    public void setUp() {
        // capacity 1 => the second live put always evicts.
        cache = new MemoryLRUCache("lru", 1);
        context = new InternalMockProcessorContext<>();
        cache.init(context, cache);
        cache.setWhenEldestRemoved((key, value) -> evictions.incrementAndGet());
    }

    @Test
    public void shouldSuppressEvictionListenerDuringRestoreAndResumeAfter() {
        // overflow during restore: eviction stays silent while restoring.
        context.restore("lru", Arrays.asList(
            KeyValue.pair(new byte[]{1}, new byte[]{1}),
            KeyValue.pair(new byte[]{2}, new byte[]{2})
        ));
        assertEquals(0, evictions.get(), "eviction listener must be suppressed during restore");

        // flag cleared after restore: eviction resumes.
        cache.put(Bytes.wrap(new byte[]{3}), new byte[]{3});
        assertEquals(1, evictions.get(), "eviction listener must resume after restore");
    }

    @Test
    public void shouldResetRestoringFlagWhenRestoreThrows() {
        // a null record makes restoreBatch throw mid-restore.
        final List<ConsumerRecord<byte[], byte[]>> failingBatch = new ArrayList<>();
        failingBatch.add(null);
        assertThrows(NullPointerException.class, () -> context.restoreWithHeaders("lru", failingBatch));

        // flag must be reset; otherwise this eviction is silently swallowed.
        cache.put(Bytes.wrap(new byte[]{1}), new byte[]{1});
        cache.put(Bytes.wrap(new byte[]{2}), new byte[]{2});
        assertEquals(1, evictions.get(), "restoring flag must be reset after a failed restore so eviction resumes");
    }
}
