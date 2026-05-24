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

/**
 * Verifies that {@link MemoryLRUCache} reacts correctly to the
 * {@link RecordBatchingStateRestoreCallback} lifecycle hooks driven by
 * the framework: the {@code restoring} flag is flipped in step with
 * {@code onRestoreStart} / {@code onRestoreEnd}, and the eviction
 * listener is suppressed only while a restore window is open.
 */
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
        assertFalse(cache.isRestoring(), "flag must default to false before any restore");

        callback.onRestoreStart();
        assertTrue(cache.isRestoring(), "onRestoreStart must set the flag");

        callback.onRestoreEnd();
        assertFalse(cache.isRestoring(), "onRestoreEnd must clear the flag");
    }

    @Test
    public void onRestoreEndClearsFlagWhenStartWasNeverCalled() {
        // Models the framework's finally branch when onRestoreStart itself threw:
        // onRestoreEnd is still invoked and must leave the store in a usable state.
        callback.onRestoreEnd();
        assertFalse(cache.isRestoring());
    }

    @Test
    public void evictionListenerIsSuppressedWhileRestoringAndResumesAfter() {
        final AtomicInteger evictions = new AtomicInteger();
        cache.setWhenEldestRemoved((k, v) -> evictions.incrementAndGet());

        // Inside a restore window: evictions are silent.
        callback.onRestoreStart();
        cache.put(Bytes.wrap(new byte[]{1}), new byte[]{1});
        cache.put(Bytes.wrap(new byte[]{2}), new byte[]{2});
        cache.put(Bytes.wrap(new byte[]{3}), new byte[]{3}); // evicts key 1, listener must NOT fire
        assertEquals(0, evictions.get(), "eviction listener must be suppressed during restore");
        callback.onRestoreEnd();

        // Outside the window: the next eviction fires the listener.
        cache.put(Bytes.wrap(new byte[]{4}), new byte[]{4}); // evicts key 2
        assertEquals(1, evictions.get(), "eviction listener must fire after restore ends");
    }

    @Test
    public void restoreBatchPopulatesMapAndKeepsListenerSuppressed() {
        final AtomicInteger evictions = new AtomicInteger();
        cache.setWhenEldestRemoved((k, v) -> evictions.incrementAndGet());

        callback.onRestoreStart();
        callback.restoreBatch(Collections.emptyList()); // empty batch is a valid lifecycle event
        callback.onRestoreEnd();

        assertEquals(0, evictions.get());
        assertFalse(cache.isRestoring());
    }
}
