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
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.test.InternalMockProcessorContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MemoryLRUCacheLifecycleTest {

    private MemoryLRUCache cache;
    private RecordBatchingStateRestoreCallback callback;

    @BeforeEach
    public void setUp() {
        cache = new MemoryLRUCache("lru", 2);
        final InternalMockProcessorContext<?, ?> context = new InternalMockProcessorContext<>();
        cache.init(context, cache);
        callback = cache.restoreCallback();
        assertNotNull(callback, "init must register a restore callback");
    }

    @Test
    public void restoringFlagFollowsLifecycleHooks() {
        assertFalse(cache.isRestoring());

        callback.onRestoreStart();
        assertTrue(cache.isRestoring());

        callback.onRestoreEnd();
        assertFalse(cache.isRestoring());
    }

    @Test
    public void onRestoreEndClearsFlagWhenStartWasNeverCalled() {
        callback.onRestoreEnd();
        assertFalse(cache.isRestoring());
    }

    @Test
    public void evictionListenerIsSuppressedWhileRestoringAndResumesAfter() {
        final AtomicInteger evictions = new AtomicInteger();
        cache.setWhenEldestRemoved((k, v) -> evictions.incrementAndGet());

        callback.onRestoreStart();
        cache.put(Bytes.wrap(new byte[]{1}), new byte[]{1});
        cache.put(Bytes.wrap(new byte[]{2}), new byte[]{2});
        cache.put(Bytes.wrap(new byte[]{3}), new byte[]{3});
        assertEquals(0, evictions.get());
        callback.onRestoreEnd();

        cache.put(Bytes.wrap(new byte[]{4}), new byte[]{4});
        assertEquals(1, evictions.get());
    }

    @Test
    public void restoreBatchPopulatesMapAndKeepsListenerSuppressed() {
        final AtomicInteger evictions = new AtomicInteger();
        cache.setWhenEldestRemoved((k, v) -> evictions.incrementAndGet());

        callback.onRestoreStart();
        callback.restoreBatch(Collections.emptyList());
        callback.onRestoreEnd();

        assertEquals(0, evictions.get());
        assertFalse(cache.isRestoring());
    }
}
