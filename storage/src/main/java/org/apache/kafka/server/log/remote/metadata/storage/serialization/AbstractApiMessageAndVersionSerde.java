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
package org.apache.kafka.server.log.remote.metadata.storage.serialization;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.metadata.ApiMessageAndVersion;

import java.nio.ByteBuffer;

/**
 * This class provides serialization/deserialization of {@code ApiMessageAndVersion}.
 * <p></p>
 * Implementors need to extend this class and implement {@link #apiMessageFor(short)} method to return a respective
 * {@code ApiMessage} for the given {@code apiKey}. This is required to deserialize the bytes to build the respective
 * {@code ApiMessage} instance.
 */
public abstract class AbstractApiMessageAndVersionSerde  {

    public byte[] serialize(ApiMessageAndVersion messageAndVersion) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        short version = messageAndVersion.version();
        ApiMessage message = messageAndVersion.message();

        // Compute total size of the data including header: apiKey and apiVersion.
        int headerSize = ByteUtils.sizeOfUnsignedVarint(messageAndVersion.message().apiKey()) +
                ByteUtils.sizeOfUnsignedVarint(messageAndVersion.version());
        int messageSize = message.size(cache, version);
        ByteBufferAccessor writable = new ByteBufferAccessor(ByteBuffer.allocate(headerSize + messageSize));

        // Write apiKey and version
        writable.writeUnsignedVarint(message.apiKey());
        writable.writeUnsignedVarint(version);

        // Write the message
        message.write(writable, cache, version);

        return writable.buffer().array();
    }

    public ApiMessageAndVersion deserialize(byte[] data) {

        ByteBufferAccessor readable = new ByteBufferAccessor(ByteBuffer.wrap(data));

        short apiKey = (short) readable.readUnsignedVarint();
        short version = (short) readable.readUnsignedVarint();

        ApiMessage message = apiMessageFor(apiKey);
        message.read(readable, version);

        return new ApiMessageAndVersion(message, version);
    }

    /**
     * Return {@code ApiMessage} instance for the given {@code apiKey}. This is used while deserializing the bytes
     * payload into the respective {@code ApiMessage} in {@link #deserialize(byte[])} method.
     *
     * @param apiKey apiKey for which a {@code ApiMessage} to be created.
     */
    public abstract ApiMessage apiMessageFor(short apiKey);

}