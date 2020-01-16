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
package org.apache.kafka.clients.producer.internals;

import com.ibm.disni.verbs.IbvMr;
import org.apache.kafka.clients.RdmaClient;

import org.apache.kafka.common.utils.Time;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;


/**
 *  TODO Optimize the concurrent memory allocator
 */
public class RdmaBufferPool {

    public final long tcpTimeout;

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    private final ConcurrentSkipListMap<Long, RdmaSegment> segments = new ConcurrentSkipListMap<Long, RdmaSegment>();

    private final int defaultSegmentSize;

    ReentrantLock lock = new ReentrantLock();

    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.  */

    private final Time time;
    private final RdmaClient rdmaClient;

    private final AtomicInteger numberOfSegments;

    /**
     * Create a new buffer pool
     *
     * @param time time instance
     */
    public RdmaBufferPool(int defaultSegmentSize,   Time time, RdmaClient rdmaClient, long tcpTimeout) {
        this.defaultSegmentSize = defaultSegmentSize;
        this.time = time;
        this.rdmaClient = rdmaClient;
        RdmaSegment segment = new RdmaSegment(defaultSegmentSize, rdmaClient);
        segments.put(segment.getStartAddress(), segment);
        numberOfSegments = new AtomicInteger(1);
        this.tcpTimeout = tcpTimeout;
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        ByteBuffer buffer = null;
        do {
            int num = numberOfSegments.get();
            for (ConcurrentSkipListMap.Entry<Long, RdmaSegment> entry : segments.entrySet()) {
                RdmaSegment segment = entry.getValue();
                buffer = segment.allocate(size, maxTimeToBlockMs);
                if (buffer != null)
                    return buffer;
            }

            lock.lock();
            try {
                int newnum = numberOfSegments.get();
                if (newnum == num) {
                   // System.out.println("allocate new buffer");
                    RdmaSegment segment = new RdmaSegment(defaultSegmentSize, rdmaClient);
                    buffer = segment.allocate(size, maxTimeToBlockMs);
                    segments.put(segment.getStartAddress(), segment);
                    numberOfSegments.incrementAndGet();
                } else {
                    System.out.println("RETRY to allocate");
                }
            } finally {
                lock.unlock();
            }
        } while (buffer == null);

        return buffer;
    }

    public int getLkey(ByteBuffer buffer) {
        long address = ((DirectBuffer) buffer).address();

        RdmaSegment segment = segments.floorEntry(address).getValue();
        return segment.getLkey();
    }



    public void deallocate(ByteBuffer buffer) {
        long address = ((DirectBuffer) buffer).address();
        RdmaSegment segment = segments.floorEntry(address).getValue();
        segment.deallocate(buffer);
    }



    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        return 0;
    }



    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return defaultSegmentSize * this.segments.size();
    }

}

class RdmaSegment {

    private long expectedFreeSize;
    private final long totalMemory;
    private final long startAddress;
    private final long endAddress;

    private long currentAddress;
    //  private long firstFreeByte; // [firstFreeByte,lastFreeByte)
    private long lastFreeByte;

    private final ByteBuffer memory;

    private final ReentrantLock lock;
    private final TreeSet<DirectBuffer> free;

    private volatile IbvMr mr;
    private volatile int lkey;
    private final RdmaClient rdmaClient;

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocateDirect(size);
    }


    public long getStartAddress() {
        return startAddress;
    }

    public RdmaSegment(int memory, RdmaClient rdmaClient) {
        this.memory = allocateByteBuffer(memory);
        this.startAddress = ((DirectBuffer) this.memory).address();
        this.endAddress = startAddress + memory;
        this.currentAddress = this.startAddress;

        this.lastFreeByte = this.endAddress;

        this.lock = new ReentrantLock();

        this.rdmaClient = rdmaClient;
        this.totalMemory = memory;
        this.expectedFreeSize = memory;
        this.lkey = -1;
        this.free = new TreeSet<>(new Comparator<DirectBuffer>() {
            @Override
            public int compare(DirectBuffer o1, DirectBuffer o2) {
                return Long.compare(o1.address(), o2.address());
            }
        });
    }
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");

        ByteBuffer buffer = null;
        this.lock.lock();
        try {
            if (expectedFreeSize < size) {
                return null;
            }

            if (currentAddress + size > endAddress) {
                // wrap around
                int position = (int) (currentAddress - startAddress);
                int length = (int) (lastFreeByte - currentAddress);
                if (length > 0) {
                    ByteBuffer lastbuffer = ((ByteBuffer) memory.duplicate().position(position).limit(position + length)).slice();
                    this.free.add((DirectBuffer) lastbuffer);
                }
                currentAddress = startAddress;
                lastFreeByte = startAddress;
            }

            if ((lastFreeByte - currentAddress) < size) {
                // not enough memory
                // need to deallocate
                while (!free.isEmpty()) {
                    DirectBuffer buf = free.first();
                    if (buf.address() == lastFreeByte) {
                        lastFreeByte += ((ByteBuffer) buf).capacity();
                        free.pollFirst();
                    } else {
                        break;
                    }
                }
            }

            if ((lastFreeByte - currentAddress) >= size) {
                int position = (int) (currentAddress - startAddress);
                buffer = ((ByteBuffer) memory.duplicate().position(position).limit(position + size)).slice();
                currentAddress += size;
            } else {
               // System.out.println("No memory in buffer " + startAddress);
            }

        } finally {
            if (buffer != null) {
                expectedFreeSize -= buffer.capacity();
            }
            lock.unlock();
        }

        return buffer;
    }

    public int getLkey() {
        if (lkey != -1) {
            return lkey;
        }
        lock.lock();
        try {
            if (lkey != -1) {
                return lkey;
            }
            // then it means that memory is not registered after all
            this.mr = rdmaClient.MemReg(memory);
            this.lkey = mr.getLkey();
            return lkey;
        } catch (Exception e) {
            System.out.println("Uncaught memory allocation error");
        } finally {
            lock.unlock();
        }
        return -1;
    }



    public void deallocate(ByteBuffer buffer) {
        lock.lock();
        try {
            buffer.clear();
            expectedFreeSize += buffer.capacity();
            this.free.add((DirectBuffer) buffer);
        } finally {
            lock.unlock();
        }
    }


}