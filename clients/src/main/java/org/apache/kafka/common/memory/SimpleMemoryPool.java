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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * a simple pool implementation. this implementation just provides a limit on the total outstanding memory.
 * any buffer allocated must be release()ed always otherwise memory is not marked as reclaimed (and "leak"s)
 */
public class SimpleMemoryPool implements MemoryPool {
    protected final Logger log = LoggerFactory.getLogger(getClass()); //subclass-friendly

    protected final long sizeBytes;
    protected final boolean strict;
    protected final AtomicLong availableMemory;
    protected final int maxSingleAllocationSize;
    protected final AtomicLong startOfNoMemPeriod = new AtomicLong(); //nanoseconds
    protected volatile Sensor oomTimeSensor;

    public SimpleMemoryPool(long sizeInBytes, int maxSingleAllocationBytes, boolean strict, Sensor oomPeriodSensor) {
        if (sizeInBytes <= 0 || maxSingleAllocationBytes <= 0 || maxSingleAllocationBytes > sizeInBytes)
            throw new IllegalArgumentException("must provide a positive size and max single allocation size smaller than size."
                + "provided " + sizeInBytes + " and " + maxSingleAllocationBytes + " respectively");
        this.sizeBytes = sizeInBytes;
        this.strict = strict;
        this.availableMemory = new AtomicLong(sizeInBytes);
        this.maxSingleAllocationSize = maxSingleAllocationBytes;
        this.oomTimeSensor = oomPeriodSensor;
    }

    @Override
    public ByteBuffer tryAllocate(int sizeBytes) {
        if (sizeBytes < 1)
            throw new IllegalArgumentException("requested size " + sizeBytes + "<=0");
        if (sizeBytes > maxSingleAllocationSize)
            throw new IllegalArgumentException("requested size " + sizeBytes + " is larger than maxSingleAllocationSize " + maxSingleAllocationSize);

        long available;
        boolean success = false;
        //in strict mode we will only allocate memory if we have at least the size required.
        //in non-strict mode we will allocate memory if we have _any_ memory available (so available memory
        //can dip into the negative and max allocated memory would be sizeBytes + maxSingleAllocationSize)
        long threshold = strict ? sizeBytes : 1;
        while ((available = availableMemory.get()) >= threshold) {
            success = availableMemory.compareAndSet(available, available - sizeBytes);
            if (success)
                break;
        }

        if (success) {
            maybeRecordEndOfDrySpell();
        } else {
            if (oomTimeSensor != null) {
                startOfNoMemPeriod.compareAndSet(0, System.nanoTime());
            }
            log.trace("refused to allocate buffer of size {}", sizeBytes);
            return null;
        }

        ByteBuffer allocated = ByteBuffer.allocate(sizeBytes);
        bufferToBeReturned(allocated);
        return allocated;
    }

    @Override
    public void release(ByteBuffer previouslyAllocated) {
        if (previouslyAllocated == null)
            throw new IllegalArgumentException("provided null buffer");

        bufferToBeReleased(previouslyAllocated);
        availableMemory.addAndGet(previouslyAllocated.capacity());
        maybeRecordEndOfDrySpell();
    }

    @Override
    public long size() {
        return sizeBytes;
    }

    @Override
    public long availableMemory() {
        return availableMemory.get();
    }

    @Override
    public boolean isOutOfMemory() {
        return availableMemory.get() <= 0;
    }

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is returned to client code.
    protected void bufferToBeReturned(ByteBuffer justAllocated) {
        log.trace("allocated buffer of size {} ", justAllocated.capacity());
    }

    //allows subclasses to do their own bookkeeping (and validation) _before_ memory is marked as reclaimed.
    protected void bufferToBeReleased(ByteBuffer justReleased) {
        log.trace("released buffer of size {}", justReleased.capacity());
    }

    @Override
    public String toString() {
        long allocated = sizeBytes - availableMemory.get();
        return "SimpleMemoryPool{" + Utils.formatBytes(allocated) + "/" + Utils.formatBytes(sizeBytes) + " used}";
    }

    protected void maybeRecordEndOfDrySpell() {
        if (oomTimeSensor != null) {
            long startOfDrySpell = startOfNoMemPeriod.getAndSet(0);
            if (startOfDrySpell != 0) {
                //how long were we refusing allocation requests for
                oomTimeSensor.record((System.nanoTime() - startOfDrySpell) / 1000000.0); //fractional (double) millis
            }
        }
    }
}
