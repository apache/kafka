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
package org.apache.kafka.server.common.serialization;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.nio.ByteBuffer;

/**
 * This class provides conversion of {@code ApiMessageAndVersion} to bytes and vice versa. This can be used as serialization protocol for any
 * metadata records derived of {@code ApiMessage}s. It internally uses {@link AbstractApiMessageSerde} for serialization/deserialization
 * mechanism.
 * <br><br>
 * Implementors need to extend this class and implement {@link #apiMessageFor(short)} method to return a respective
 * {@code ApiMessage} for the given {@code apiKey}. This is required to deserialize the bytes to build the respective
 * {@code ApiMessage} instance.
 */
public abstract class BytesApiMessageSerde {

    private final AbstractApiMessageSerde apiMessageSerde = new AbstractApiMessageSerde() {
        @Override
        public ApiMessage apiMessageFor(short apiKey) {
            return BytesApiMessageSerde.this.apiMessageFor(apiKey);
        }
    };

    public byte[] serialize(ApiMessageAndVersion messageAndVersion) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = apiMessageSerde.recordSize(messageAndVersion, cache);
        ByteBufferAccessor writable = new ByteBufferAccessor(ByteBuffer.allocate(size));
        apiMessageSerde.write(messageAndVersion, cache, writable);

        return writable.buffer().array();
    }

    public ApiMessageAndVersion deserialize(byte[] data) {
        Readable readable = new ByteBufferAccessor(ByteBuffer.wrap(data));

        return apiMessageSerde.read(readable, data.length);
    }

    /**
     * Return {@code ApiMessage} instance for the given {@code apiKey}. This is used while deserializing the bytes
     * payload into the respective {@code ApiMessage} in {@link #deserialize(byte[])} method.
     *
     * @param apiKey apiKey for which a {@code ApiMessage} to be created.
     */
    public abstract ApiMessage apiMessageFor(short apiKey);

}