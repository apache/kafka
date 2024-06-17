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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A response header in the kafka protocol.
 */
public class ResponseHeader implements AbstractRequestResponse {
    private final static int SIZE_NOT_INITIALIZED = -1;
    private final ResponseHeaderData data;
    private final short headerVersion;
    private int size = SIZE_NOT_INITIALIZED;

    public ResponseHeader(int correlationId, short headerVersion) {
        this(new ResponseHeaderData().setCorrelationId(correlationId), headerVersion);
    }

    public ResponseHeader(ResponseHeaderData data, short headerVersion) {
        this.data = data;
        this.headerVersion = headerVersion;
    }

    /**
     * Calculates the size of {@link ResponseHeader} in bytes.
     *
     * This method to calculate size should be only when it is immediately followed by
     * {@link #write(ByteBuffer, ObjectSerializationCache)} method call. In such cases, ObjectSerializationCache
     * helps to avoid the serialization twice. In all other cases, {@link #size()} should be preferred instead.
     *
     * Calls to this method leads to calculation of size every time it is invoked. {@link #size()} should be preferred
     * instead.
     *
     * Visible for testing.
     */
    int size(ObjectSerializationCache serializationCache) {
        return data().size(serializationCache, headerVersion);
    }

    /**
     * Returns the size of {@link ResponseHeader} in bytes.
     *
     * Calls to this method are idempotent and inexpensive since it returns the cached value of size after the first
     * invocation.
     */
    public int size() {
        if (this.size == SIZE_NOT_INITIALIZED) {
            this.size = size(new ObjectSerializationCache());
        }
        return size;
    }

    public int correlationId() {
        return this.data.correlationId();
    }

    public short headerVersion() {
        return headerVersion;
    }

    public ResponseHeaderData data() {
        return data;
    }

    // visible for testing
    void write(ByteBuffer buffer, ObjectSerializationCache serializationCache) {
        data.write(new ByteBufferAccessor(buffer), serializationCache, headerVersion);
    }

    @Override
    public String toString() {
        return "ResponseHeader("
            + "correlationId=" + data.correlationId()
            + ", headerVersion=" + headerVersion
            + ")";
    }

    public static ResponseHeader parse(ByteBuffer buffer, short headerVersion) {
        final int bufferStartPositionForHeader = buffer.position();
        final ResponseHeader header = new ResponseHeader(
            new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion), headerVersion);
        // Size of header is calculated by the shift in the position of buffer's start position during parsing.
        // Prior to parsing, the buffer's start position points to header data and after the parsing operation
        // the buffer's start position points to api message. For more information on how the buffer is
        // constructed, see RequestUtils#serialize()
        header.size = Math.max(buffer.position() - bufferStartPositionForHeader, 0);
        return header;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponseHeader that = (ResponseHeader) o;
        return headerVersion == that.headerVersion &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, headerVersion);
    }
}
