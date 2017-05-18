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

import org.apache.kafka.clients.producer.BufferExhaustedException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class MmapBufferPool implements BufferPool {
    
    private final long maxSize;
    private final int chunkSize;
    /** This memory is accounted for separately from the poolable buffers in free. */
    private long availableMemory;

    private final BlockingDeque<ByteBuffer> free;
    private MappedByteBuffer fileBuffer;
    
    public MmapBufferPool(File backingFileName, long maxSize, int chunkSize) throws IOException {
        this.maxSize = maxSize;
        this.availableMemory = maxSize;
        this.chunkSize = chunkSize;
        this.free = new LinkedBlockingDeque<ByteBuffer>();

        RandomAccessFile f = new RandomAccessFile(backingFileName, "rw");
        f.setLength(maxSize);

        this.fileBuffer = f.getChannel().map(MapMode.READ_WRITE, 0, maxSize);
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
        if (size > chunkSize)
            throw new IllegalArgumentException("Illegal allocation size.");

        // check if we have a free buffer of the right size pooled
        if (size == chunkSize && !this.free.isEmpty())
            return this.free.pollFirst();

        // now check if the request is immediately satisfiable with the
        // memory on hand or if we need to block
        int freeListSize = freeSize() * this.chunkSize;
        if (this.availableMemory + freeListSize >= size) {
            // we have enough unallocated or pooled memory to immediately
            // satisfy the request
            freeUp(size);
            ByteBuffer allocatedBuffer = allocateByteBuffer(size);
            this.availableMemory -= size;
            return allocatedBuffer;
        } else {
            // we are out of memory and will have to block
            throw new BufferExhaustedException("You have exhausted the " + this.maxSize
                        + " bytes of memory you configured for the client and the client is configured to error"
                        + " rather than block when memory is exhausted.");
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        ByteBuffer fileBufferSlice = this.fileBuffer.slice();
        fileBufferSlice.limit(chunkSize);
        this.fileBuffer.position(this.fileBuffer.position() + chunkSize);

        return fileBufferSlice;
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.availableMemory < size)
            this.availableMemory += this.free.pollLast().capacity();
    }

    @Override
    public void deallocate(ByteBuffer buffer, int size) {
        // TODO: check size
        this.free.add(buffer);
    }

    @Override
    public void deallocate(ByteBuffer buffer) {
        // TODO: check size
        this.free.add(buffer);
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    @Override
    public long availableMemory() {
        return this.availableMemory + freeSize() * (long) this.chunkSize;
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    @Override
    public long unallocatedMemory() {
        return this.availableMemory;
    }

    @Override
    public int queued() {
     // TODO write me
        return 0;
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
