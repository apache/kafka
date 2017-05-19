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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MmapBufferPool implements BufferPool {
    
    private final long totalMemory;
    private final int chunkSize;
    /** This memory is accounted for separately from the poolable buffers in free. */
    private long availableMemory;
    private final Time time;

    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final Deque<Condition> waiters;
    private MappedByteBuffer fileBuffer;
    
    public MmapBufferPool(long totalMemory, int chunkSize, Time time, File backingFileName) throws IOException {
        this.chunkSize = chunkSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = totalMemory;
        this.availableMemory = totalMemory;
        this.time = time;

        RandomAccessFile f = new RandomAccessFile(backingFileName, "rw");
        f.setLength(totalMemory);

        this.fileBuffer = f.getChannel().map(MapMode.READ_WRITE, 0, totalMemory);
//        while (this.fileBuffer.remaining() >= chunkSize) {
//            ByteBuffer fileBufferSlice = this.fileBuffer.slice();
//            fileBufferSlice.limit(chunkSize);
//            this.fileBuffer.position(this.fileBuffer.position() + chunkSize);
//            free.add(fileBufferSlice);
//        }
        f.close();
    }

    @Override
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            if (size == chunkSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            int freeListSize = freeSize() * this.chunkSize;
            if (this.availableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request
//                freeUp(size);
                ByteBuffer allocatedBuffer = allocateByteBuffer(size);
                this.availableMemory -= size;
                return allocatedBuffer;
            } else {
                // we are out of memory and will have to block
                int accumulated = 0;
                ByteBuffer buffer = null;
                boolean hasError = true;
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
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

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        if (accumulated == 0 && size == this.chunkSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
//                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.availableMemory);
                            this.availableMemory -= got;
                            accumulated += got;
                        }
                    }

                    if (buffer == null)
                        buffer = allocateByteBuffer(size);
                    hasError = false;
                    //unlock happens in top-level, enclosing finally
                    return buffer;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    if (hasError)
                        this.availableMemory += accumulated;
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                if (!(this.availableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        ByteBuffer fileBufferSlice = this.fileBuffer.slice();
        fileBufferSlice.limit(size);
        this.fileBuffer.position(this.fileBuffer.position() + size);

        return fileBufferSlice;
    }

    @Override
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            int sliceSize = buffer.limit() - buffer.position();
            if (size == this.chunkSize && size == sliceSize) {
                this.free.add(buffer);
            } else {
                this.availableMemory += size;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deallocate(ByteBuffer buffer) {
        int size = buffer.limit() - buffer.position();
        deallocate(buffer, size);
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    @Override
    public long availableMemory() {
        lock.lock();
        try {
            return this.availableMemory + freeSize() * (long) this.chunkSize;
        } finally {
          lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    @Override
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.availableMemory;
        } finally {
            lock.unlock();
        }
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
     // TODO write me
        return 0;
    }

    @Override
    public long totalMemory() {
     // TODO write me
        return 0;
    }

}
