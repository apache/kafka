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

import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BounceBufferPool implements AutoCloseable {
    private final Logger log;

    private final BounceBufferFactory bufferFactory;

    private final int maxBufferSize;

    private final int maxBuffers;

    private int curBuffers;

    private boolean closed;

    private final List<ByteBuffer> buffers;

    public BounceBufferPool(Logger log,
                            BounceBufferFactory bufferFactory,
                            int maxBufferSize,
                            int maxBuffers) {
        this.log = log;
        this.bufferFactory = bufferFactory;
        this.maxBufferSize = maxBufferSize;
        this.maxBuffers = maxBuffers;
        this.curBuffers = 0;
        this.closed = false;
        this.buffers = new ArrayList<>(maxBuffers);
    }

    public ByteBuffer allocate() throws InterruptedException {
        synchronized (this) {
            while (true) {
                if (closed) {
                    throw new RuntimeException("The BounceBuffers pool has been closed.");
                }
                if (!this.buffers.isEmpty()) {
                    return this.buffers.remove(this.buffers.size() - 1);
                }
                if (curBuffers < maxBuffers) {
                    break;
                }
                this.wait();
            }
            curBuffers++;
        }
        ByteBuffer buffer = null;
        try {
            buffer = bufferFactory.allocate(maxBufferSize);
        } catch (Throwable t) {
            log.warn("Failed to allocate BounceBuffer", t);
            synchronized (this) {
                curBuffers--;
            }
            throw t;
        }
        synchronized (this) {
            if (closed) {
                curBuffers--;
                bufferFactory.free(buffer);
                throw new RuntimeException("The BounceBuffers pool has been closed.");
            }
        }
        return buffer;
    }

    // Visible for testing
    int curBuffers() {
        return curBuffers;
    }

    public synchronized void release(ByteBuffer buffer) {
        if (log.isTraceEnabled()) {
            log.trace("Releasing bounce buffer {}", buffer);
        }
        if (closed) {
            bufferFactory.free(buffer);
        } else {
            this.buffers.add(buffer);
            this.notify();
        }
    }

    public synchronized void close() {
        log.debug("Closing BounceBuffers");
        if (!closed) {
            closed = true;
            for (ByteBuffer buffer : buffers) {
                bufferFactory.free(buffer);
            }
            buffers.clear();
            curBuffers = 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder bld = new StringBuilder();
        bld.append("BounceBuffers").
            append("(bufferFactory=").append(bufferFactory).
            append(", maxBufferSize=").append(maxBufferSize).
            append(", maxBuffers=").append(maxBuffers);
        synchronized (this) {
            bld.append(", curBuffers=").append(curBuffers).
                append(", closed=").append(closed);
        }
        bld.append(")");
        return bld.toString();
    }
}
