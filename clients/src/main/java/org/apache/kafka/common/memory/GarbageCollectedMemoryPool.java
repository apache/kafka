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

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * An extension of SimpleMemoryPool that tracks allocated buffers and logs an error when they "leak"
 * (when they are garbage-collected without having been release()ed).
 * THIS IMPLEMENTATION IS A DEVELOPMENT/DEBUGGING AID AND IS NOT MEANT PRO PRODUCTION USE.
 */
public class GarbageCollectedMemoryPool extends SimpleMemoryPool implements AutoCloseable {

    private final ReferenceQueue<ByteBuffer> garbageCollectedBuffers = new ReferenceQueue<>();
    //serves 2 purposes - 1st it maintains the ref objects reachable (which is a requirement for them
    //to ever be enqueued), 2nd keeps some (small) metadata for every buffer allocated
    private final Map<BufferReference, BufferMetadata> buffersInFlight = new ConcurrentHashMap<>();
    private final Thread gcListenerThread;
    private volatile boolean alive = true;

    public GarbageCollectedMemoryPool(long sizeBytes, int maxSingleAllocationSize, boolean strict, Sensor oomPeriodSensor) {
        super(sizeBytes, maxSingleAllocationSize, strict, oomPeriodSensor);
        GarbageCollectionListener gcListener = new GarbageCollectionListener();
        this.gcListenerThread = new Thread(gcListener, "memory pool GC listener");
        this.gcListenerThread.setDaemon(true); //so we dont need to worry about shutdown
        this.gcListenerThread.start();
    }

    @Override
    protected void bufferToBeReturned(ByteBuffer justAllocated) {
        BufferReference ref = new BufferReference(justAllocated, garbageCollectedBuffers);
        BufferMetadata metadata = new BufferMetadata(justAllocated.capacity());
        if (buffersInFlight.put(ref, metadata) != null)
            //this is a bug. it means either 2 different co-existing buffers got
            //the same identity or we failed to register a released/GC'ed buffer
            throw new IllegalStateException("allocated buffer identity " + ref.hashCode + " already registered as in use?!");

        log.trace("allocated buffer of size {} and identity {}", sizeBytes, ref.hashCode);
    }

    @Override
    protected void bufferToBeReleased(ByteBuffer justReleased) {
        BufferReference ref = new BufferReference(justReleased); //used ro lookup only
        BufferMetadata metadata = buffersInFlight.remove(ref);
        if (metadata == null)
            //its impossible for the buffer to have already been GC'ed (because we have a hard ref to it
            //in the function arg) so this means either a double free or not our buffer.
            throw new IllegalArgumentException("returned buffer " + ref.hashCode + " was never allocated by this pool");
        if (metadata.sizeBytes != justReleased.capacity()) {
            //this is a bug
            throw new IllegalStateException("buffer " + ref.hashCode + " has capacity " + justReleased.capacity() + " but recorded as " + metadata.sizeBytes);
        }
        log.trace("released buffer of size {} and identity {}", metadata.sizeBytes, ref.hashCode);
    }

    @Override
    public void close() {
        alive = false;
        gcListenerThread.interrupt();
    }

    private class GarbageCollectionListener implements Runnable {
        @Override
        public void run() {
            while (alive) {
                try {
                    BufferReference ref = (BufferReference) garbageCollectedBuffers.remove(); //blocks
                    ref.clear();
                    //this cannot race with a release() call because an object is either reachable or not,
                    //release() can only happen before its GC'ed, and enqueue can only happen after.
                    //if the ref was enqueued it must then not have been released
                    BufferMetadata metadata = buffersInFlight.remove(ref);

                    if (metadata == null) {
                        //it can happen rarely that the buffer was release()ed properly (so no metadata) and yet
                        //the reference object to it remains reachable for a short period of time after release()
                        //and hence gets enqueued. this is because we keep refs in a ConcurrentHashMap which cleans
                        //up keys lazily.
                        continue;
                    }

                    availableMemory.addAndGet(metadata.sizeBytes);
                    log.error("Reclaimed buffer of size {} and identity {} that was not properly release()ed. This is a bug.", metadata.sizeBytes, ref.hashCode);
                } catch (InterruptedException e) {
                    log.debug("interrupted", e);
                    //ignore, we're a daemon thread
                }
            }
            log.info("GC listener shutting down");
        }
    }

    private static final class BufferMetadata {
        private final int sizeBytes;

        private BufferMetadata(int sizeBytes) {
            this.sizeBytes = sizeBytes;
        }
    }

    private static final class BufferReference extends WeakReference<ByteBuffer> {
        private final int hashCode;

        private BufferReference(ByteBuffer referent) { //used for lookup purposes only - no queue required.
            this(referent, null);
        }

        private BufferReference(ByteBuffer referent, ReferenceQueue<? super ByteBuffer> q) {
            super(referent, q);
            hashCode = System.identityHashCode(referent);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) { //this is important to find leaked buffers (by ref identity)
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BufferReference that = (BufferReference) o;
            if (hashCode != that.hashCode) {
                return false;
            }
            ByteBuffer thisBuf = get();
            if (thisBuf == null) {
                //our buffer has already been GC'ed, yet "that" is not us. so not same buffer
                return false;
            }
            ByteBuffer thatBuf = that.get();
            return thisBuf == thatBuf;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    @Override
    public String toString() {
        long allocated = sizeBytes - availableMemory.get();
        return "GarbageCollectedMemoryPool{" + Utils.formatBytes(allocated) + "/" + Utils.formatBytes(sizeBytes) + " used in " + buffersInFlight.size() + " buffers}";
    }
}
