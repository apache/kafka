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


import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MmapBufferPool implements BufferPool {
    private final long totalMemory;
    private final int poolableSize;
    private final Time time;

    private final ReentrantLock lock;
    private final FreeList free;
    private final Deque<Condition> waiters;
    
    public MmapBufferPool(long totalMemory, int poolableSize, Time time, File backingFileName) throws IOException {
        this.totalMemory = totalMemory;
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.waiters = new ArrayDeque<>();
        this.time = time;

        RandomAccessFile f = new RandomAccessFile(backingFileName, "rw");
        f.setLength(totalMemory);

        MappedByteBuffer fileBuffer = f.getChannel().map(MapMode.READ_WRITE, 0, totalMemory);
        f.close();

        this.free = new FreeList(fileBuffer);
    }

    @Override
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        lock.lock();
        try {
            if (this.free.max() >= size) {
                return this.free.find(size);
            } else {
                // we are out of memory and will have to block
                long accumulated = 0;
                ByteBuffer buffer = null;
                boolean hasError = true;
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (this.free.max() < size) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
//                            this.waitTime.record(timeNs, time.milliseconds());
                        }

                        if (waitingTimeElapsed) {
                            throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        this.free.find(size);
                    }

//                    if (buffer == null)
//                        buffer = allocateByteBuffer(size);
                    hasError = false;
                    //unlock happens in top-level, enclosing finally
                    return buffer;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    if (hasError) {
//                        this.availableMemory += accumulated;
                    }
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                if (!(this.availableMemory() == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }
    }

    @Override
    public void deallocate(ByteBuffer buffer, int size) {
        throw new NotImplementedException();
    }

    @Override
    public void deallocate(ByteBuffer buffer) {
        lock.lock();
        try {
            this.free.add(buffer);
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    @Override
    public long availableMemory() {
        lock.lock();
        try {
            return this.free.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long unallocatedMemory() {
        // TODO write me
        return 0;
    }

    /**
     * The number of threads blocked waiting on memory
     */
    @Override
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int poolableSize() {
        return this.free.size();
    }

    @Override
    public long totalMemory() {
     // TODO write me
        return 0;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}
