/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;

/**
 * A pool of byte buffers that will be reused
 *
 */
public interface BufferPool {

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     * 
     * @param size The buffer size to allocate in bytes
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     * @throws BufferExhaustedException if the pool is in non-blocking mode and size exceeds the free memory in the pool
     */
    public ByteBuffer allocate(int size) throws InterruptedException;

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this maybe smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size);

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     * 
     * @param buffer The buffer to return
     */
    public void deallocate(ByteBuffer buffer);

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory();

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory();

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued();

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize();

    /**
     * The total memory managed by this pool
     */
    public long totalMemory();

}