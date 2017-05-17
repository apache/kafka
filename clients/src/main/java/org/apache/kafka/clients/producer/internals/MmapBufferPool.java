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
    private final BlockingDeque<ByteBuffer> free;
    
    public MmapBufferPool(File backingFileName, long maxSize, int chunkSize) throws IOException {
        this.maxSize = maxSize;
        this.chunkSize = chunkSize;
        this.free = new LinkedBlockingDeque<ByteBuffer>();
        RandomAccessFile f = new RandomAccessFile(backingFileName, "rw");
        f.setLength(maxSize);
        MappedByteBuffer buffer = f.getChannel().map(MapMode.READ_WRITE, 0, maxSize);
        while (buffer.remaining() >= chunkSize) {
            ByteBuffer b = buffer.slice();
            b.limit(chunkSize);
            buffer.position(buffer.position() + chunkSize);
            free.add(b);
        }
        f.close();
    }

    @Override
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > chunkSize)
            throw new IllegalArgumentException("Illegal allocation size.");
        ByteBuffer buffer;
//        if (blockOnExhaustion) {
//            buffer = this.free.take();
//        } else {
//            buffer = this.free.poll();
//            if (buffer == null) {
//                throw new BufferExhaustedException("You have exhausted the " + this.maxSize
//                        + " bytes of memory you configured for the client and the client is configured to error"
//                        + " rather than block when memory is exhausted.");
//            }
//        }
        buffer = this.free.poll();
            
        return buffer;
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

    @Override
    public long availableMemory() {
        // TODO write me
        return 0;
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    @Override
    public long unallocatedMemory() {
        // TODO write me
        return 0;
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
