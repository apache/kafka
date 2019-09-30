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

package org.apache.kafka.common.memory;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;


/**
 * An implementation of memory pool with weak references.
 *
 * Note: This will be a no-op for all the other clients.
 *
 * If and when the GC runs, the buffers will be reclaimed and no side effects will be seen.
 */
public class WeakMemoryPool implements MemoryPool {
    private final NavigableMap<Integer, LinkedList<WeakReference<ByteBuffer>>> cache;
    private final Object lock = new Object();

    public WeakMemoryPool() {
        cache = new TreeMap<>();
    }

    /**
     * Tries to acquire a ByteBuffer of the specified size
     * @param sizeBytes size required
     * @return a ByteBuffer (which later needs to be release()ed), or null if no memory available.
     *         the buffer will be of the exact size requested, even if backed by a larger chunk of memory
     */
    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("The buffer size cannot be less than 0");
        }
        ByteBuffer buffer = null;
        synchronized (lock) {
            NavigableMap<Integer, LinkedList<WeakReference<ByteBuffer>>> tailMap = cache.tailMap(sizeBytes, true);
            LinkedList<Integer> keysToDelete = new LinkedList<>();
            for (Map.Entry<Integer, LinkedList<WeakReference<ByteBuffer>>> entry : tailMap.entrySet()) {

                LinkedList<WeakReference<ByteBuffer>> queue = entry.getValue();
                while (!queue.isEmpty() && buffer == null) {
                    buffer = queue.pop().get();
                }

                if (queue.isEmpty()) {
                    keysToDelete.add(entry.getKey());
                }

                // If buffer is non-null; break out of the loop!
                if (buffer != null) {
                    break;
                }
            }

            // Remove all keys from the map which have zero queue lengths
            for (Integer key : keysToDelete) {
                cache.remove(key);
            }
        }

        // If buffer is non-null, clear it before returning back to the user!
        if (buffer != null) {
            buffer.clear();
            return buffer;
        }

        return ByteBuffer.allocate(sizeBytes);
    }

    /**
     * Returns a previously allocated buffer to the pool.
     * @param previouslyAllocated a buffer previously returned from tryAllocate()
     */
    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null) {
            throw new IllegalArgumentException("the buffer to be released cannot be null!");
        }

        synchronized (lock) {
            LinkedList<WeakReference<ByteBuffer>> queue =
                cache.computeIfAbsent(previouslyAllocated.capacity(), k -> new LinkedList<>());
            queue.add(new WeakReference<>(previouslyAllocated));
        }
    }

    /**
     * Returns the total size of this pool
     * @return total size, in bytes
     */
    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    /**
     * Returns the amount of memory available for allocation by this pool.
     * NOTE: result may be negative (pools may over allocate to avoid starvation issues)
     * @return bytes available
     */
    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    /**
     * Returns true if the pool cannot currently allocate any more buffers
     * - meaning total outstanding buffers meets or exceeds pool size and
     * some would need to be released before further allocations are possible.
     *
     * This is equivalent to availableMemory() <= 0
     * @return true if out of memory
     */
    @Override
    public boolean isOutOfMemory() {
        return false;
    }
}
