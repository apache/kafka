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

import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.common.metrics.Sensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of memory pool which recycles buffers of commonly used size.
 * This memory pool is useful if most of the requested buffers' size are within close size range.
 * In this case, instead of deallocate and reallocate the buffer, the memory pool will recycle the buffer for future use.
 */
public class RecyclingMemoryPool implements MemoryPool {
    protected static final Logger log = LoggerFactory.getLogger(RecyclingMemoryPool.class);
    protected final int cacheableBufferSizeUpperThreshold;
    protected final int cacheableBufferSizeLowerThreshold;
    protected final LinkedBlockingQueue<ByteBuffer> bufferCache;
    protected final Sensor requestSensor;

    public RecyclingMemoryPool(int cacheableBufferSize, int bufferCacheCapacity, Sensor requestSensor) {
        if (bufferCacheCapacity <= 0 || cacheableBufferSize <= 0) {
            throw new IllegalArgumentException(String.format("Must provide a positive cacheable buffer size and buffer cache " +
                    "capacity, provided %d and %d respectively.", cacheableBufferSize, bufferCacheCapacity));
        }
        this.bufferCache = new LinkedBlockingQueue<>(bufferCacheCapacity);
        for (int i = 0; i < bufferCacheCapacity; i++) {
            bufferCache.offer(ByteBuffer.allocate(cacheableBufferSize));
        }
        this.cacheableBufferSizeUpperThreshold = cacheableBufferSize;
        this.cacheableBufferSizeLowerThreshold = cacheableBufferSize / 2;
        this.requestSensor = requestSensor;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < 1) {
            throw new IllegalArgumentException("requested size " + sizeBytes + "<=0");
        }

        ByteBuffer allocated = null;
        if (sizeBytes > cacheableBufferSizeLowerThreshold  && sizeBytes <= cacheableBufferSizeUpperThreshold) {
            allocated = bufferCache.poll();
        }
        if (allocated != null) {
            allocated.limit(sizeBytes);
        } else {
            allocated = ByteBuffer.allocate(sizeBytes);
        }
        bufferToBeAllocated(allocated);
        return allocated;
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null) {
            throw new IllegalArgumentException("provided null buffer");
        }
        if (previouslyAllocated.capacity() == cacheableBufferSizeUpperThreshold) {
            previouslyAllocated.clear();
            bufferCache.offer(previouslyAllocated);
        } else {
            bufferToBeReleased(previouslyAllocated);
        }
    }

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is returned to client code.
    protected void bufferToBeAllocated(ByteBuffer justAllocated) {
        try {
            this.requestSensor.record(justAllocated.limit());
        } catch (Exception e) {
            log.debug("failed to record size of allocated buffer");
        }
        log.trace("allocated buffer of size {}", justAllocated.capacity());
    }

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is marked as reclaimed.
    protected void bufferToBeReleased(ByteBuffer justReleased) {
        log.trace("released buffer of size {}", justReleased.capacity());
    }

    @Override
    public long size() {
        return Long.MAX_VALUE;
    }

    @Override
    public long availableMemory() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isOutOfMemory() {
        return false;
    }
}
