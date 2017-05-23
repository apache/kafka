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


import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class MmapBufferPool implements BufferPool {
    
    private final long totalMemory;
    private final int chunkSize;
    /** This memory is accounted for separately from the poolable buffers in free. */
//    private long availableMemory;

    private final BlockingDeque<ByteBuffer> free;
//    private MappedByteBuffer fileBuffer;
    
    public MmapBufferPool(long totalMemory, int chunkSize, File backingFileName) throws IOException {
        this.totalMemory = totalMemory;
        this.chunkSize = chunkSize;
        this.free = new LinkedBlockingDeque<ByteBuffer>();

        RandomAccessFile f = new RandomAccessFile(backingFileName, "rw");
        f.setLength(totalMemory);

//        this.fileBuffer = f.getChannel().map(MapMode.READ_WRITE, 0, totalMemory);
//        f.close();
    }

    @Override
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        return allocateByteBuffer(size);
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        throw new NotImplementedException();
    }

    @Override
    public void deallocate(ByteBuffer buffer, int size) {
        throw new NotImplementedException();
    }

    @Override
    public void deallocate(ByteBuffer buffer) {
        throw new NotImplementedException();
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    @Override
    public long availableMemory() {
        // TODO write me
        return 0;
    }

    // Protected for testing.
    protected int freeSize() {
        // TODO write me
        return 0;
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
