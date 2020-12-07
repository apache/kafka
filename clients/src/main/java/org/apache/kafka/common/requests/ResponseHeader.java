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
    private final ResponseHeaderData data;
    private final short headerVersion;

    public ResponseHeader(int correlationId, short headerVersion) {
        this(new ResponseHeaderData().setCorrelationId(correlationId), headerVersion);
    }

    public ResponseHeader(ResponseHeaderData data, short headerVersion) {
        this.data = data;
        this.headerVersion = headerVersion;
    }

    public int size(ObjectSerializationCache serializationCache) {
        return data().size(serializationCache, headerVersion);
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

    public void write(ByteBuffer buffer, ObjectSerializationCache serializationCache) {
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
        return new ResponseHeader(
            new ResponseHeaderData(new ByteBufferAccessor(buffer), headerVersion),
                headerVersion);
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
