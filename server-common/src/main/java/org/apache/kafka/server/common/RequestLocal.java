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
package org.apache.kafka.server.common;

import org.apache.kafka.common.utils.BufferSupplier;

import java.util.Objects;

/**
 * Container for stateful instances where the lifecycle is scoped to one request.
 * When each request is handled by one thread, efficient data structures with no locking or atomic operations
 * can be used (see RequestLocal.withThreadConfinedCaching).
 */
public class RequestLocal implements AutoCloseable {
    private static final RequestLocal NO_CACHING = new RequestLocal(BufferSupplier.NO_CACHING);

    private final BufferSupplier bufferSupplier;

    public RequestLocal(BufferSupplier bufferSupplier) {
        this.bufferSupplier = bufferSupplier;
    }

    public static RequestLocal noCaching() {
        return NO_CACHING;
    }

    /** The returned instance should be confined to a single thread. */
    public static RequestLocal withThreadConfinedCaching() {
        return new RequestLocal(BufferSupplier.create());
    }

    public BufferSupplier bufferSupplier() {
        return bufferSupplier;
    }

    @Override
    public void close() {
        bufferSupplier.close();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestLocal that = (RequestLocal) o;
        return Objects.equals(bufferSupplier, that.bufferSupplier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(bufferSupplier);
    }

    @Override
    public String toString() {
        return "RequestLocal(bufferSupplier=" + bufferSupplier + ')';
    }
}
