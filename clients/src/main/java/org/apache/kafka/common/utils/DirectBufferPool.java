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
package org.apache.kafka.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pool for direct ByteBuffers used in remote storage reads.
 * Uses WeakReferences so GC can reclaim buffers under memory pressure.
 */
public class DirectBufferPool {
    private static final Logger log = LoggerFactory.getLogger(DirectBufferPool.class);
    private static final Cleaner CLEANER = Cleaner.create();

    private final ConcurrentMap<Integer, Queue<WeakReference<ByteBuffer>>> buffersBySize;
    private final AtomicLong allocations = new AtomicLong(0);
    private final AtomicLong poolHits = new AtomicLong(0);
    private final AtomicLong directAllocFailures = new AtomicLong(0);
    private final AtomicLong autoReleases = new AtomicLong(0);
    private final boolean poolingEnabled;

    public DirectBufferPool(boolean poolingEnabled) {
        this.poolingEnabled = poolingEnabled;
        this.buffersBySize = new ConcurrentHashMap<>();
    }

    public ByteBuffer allocate(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be positive");
        }

        allocations.incrementAndGet();

        if (!poolingEnabled) {
            return allocateDirect(size);
        }

        Queue<WeakReference<ByteBuffer>> queue = buffersBySize.get(size);
        if (queue != null) {
            WeakReference<ByteBuffer> ref;
            while ((ref = queue.poll()) != null) {
                ByteBuffer buf = ref.get();
                if (buf != null) {
                    poolHits.incrementAndGet();
                    buf.clear();
                    return buf;
                }
            }
        }

        return allocateDirect(size);
    }

    public void release(ByteBuffer buffer) {
        doRelease(buffer, false);
    }

    public void registerForAutoRelease(Object ref, ByteBuffer buffer) {
        if (ref == null || buffer == null || !poolingEnabled || !buffer.isDirect()) {
            return;
        }
        CLEANER.register(ref, () -> doRelease(buffer, true));
    }

    private void doRelease(ByteBuffer buffer, boolean auto) {
        if (buffer == null || !poolingEnabled || !buffer.isDirect()) {
            return;
        }

        if (auto) {
            autoReleases.incrementAndGet();
        }

        buffer.clear();
        int size = buffer.capacity();

        Queue<WeakReference<ByteBuffer>> queue = buffersBySize.get(size);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<>();
            Queue<WeakReference<ByteBuffer>> existing = buffersBySize.putIfAbsent(size, queue);
            if (existing != null) {
                queue = existing;
            }
        }

        queue.add(new WeakReference<>(buffer));
    }

    public void close() {
        buffersBySize.clear();
    }

    public long allocations() {
        return allocations.get();
    }

    public long poolHits() {
        return poolHits.get();
    }

    public long directAllocFailures() {
        return directAllocFailures.get();
    }

    public long autoReleases() {
        return autoReleases.get();
    }

    public double poolHitRate() {
        long total = allocations.get();
        return total > 0 ? (double) poolHits.get() / total : 0.0;
    }

    int countBuffersOfSize(int size) {
        Queue<WeakReference<ByteBuffer>> queue = buffersBySize.get(size);
        if (queue == null) {
            return 0;
        }

        int count = 0;
        for (WeakReference<ByteBuffer> ref : queue) {
            if (ref.get() != null) {
                count++;
            }
        }
        return count;
    }

    private ByteBuffer allocateDirect(int size) {
        try {
            return ByteBuffer.allocateDirect(size);
        } catch (OutOfMemoryError e) {
            directAllocFailures.incrementAndGet();
            log.warn("Failed to allocate direct buffer of {} bytes, using heap", size);
            return ByteBuffer.allocate(size);
        }
    }
}
