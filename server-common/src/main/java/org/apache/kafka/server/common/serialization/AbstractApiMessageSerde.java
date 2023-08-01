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
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.server.common.ApiMessageAndVersion;

/**
 * This is an implementation of {@code RecordSerde} with {@link ApiMessageAndVersion} but implementors need to implement
 * {@link #apiMessageFor(short)} to return a {@code ApiMessage} instance for the given {@code apiKey}.
 *
 * This can be used as the underlying serialization mechanism for records defined with {@link ApiMessage}s.
 * <p></p>
 * Serialization format for the given {@code ApiMessageAndVersion} is below:
 * <p></p>
 * <pre>
 *     [data_frame_version header message]
 *     header =&gt; [api_key version]
 *
 *     data_frame_version   : This is the header version, current value is 0. Header includes both api_key and version.
 *     api_key              : apiKey of {@code ApiMessageAndVersion} object.
 *     version              : version of {@code ApiMessageAndVersion} object.
 *     message              : serialized message of {@code ApiMessageAndVersion} object.
 * </pre>
 */
public abstract class AbstractApiMessageSerde implements RecordSerde<ApiMessageAndVersion> {
    private static final short DEFAULT_FRAME_VERSION = 1;
    private static final int DEFAULT_FRAME_VERSION_SIZE = ByteUtils.sizeOfUnsignedVarint(DEFAULT_FRAME_VERSION);

    private static short unsignedIntToShort(Readable input, String entity) {
        int val;
        try {
            val = input.readUnsignedVarint();
        } catch (Exception e) {
            throw new MetadataParseException("Error while reading " + entity, e);
        }
        if (val > Short.MAX_VALUE) {
            throw new MetadataParseException("Value for " + entity + " was too large.");
        }
        return (short) val;
    }

    @Override
    public int recordSize(ApiMessageAndVersion data,
                          ObjectSerializationCache serializationCache) {
        int size = DEFAULT_FRAME_VERSION_SIZE;
        size += ByteUtils.sizeOfUnsignedVarint(data.message().apiKey());
        size += ByteUtils.sizeOfUnsignedVarint(data.version());
        size += data.message().size(serializationCache, data.version());
        return size;
    }

    @Override
    public void write(ApiMessageAndVersion data,
                      ObjectSerializationCache serializationCache,
                      Writable out) {
        out.writeUnsignedVarint(DEFAULT_FRAME_VERSION);
        out.writeUnsignedVarint(data.message().apiKey());
        out.writeUnsignedVarint(data.version());
        data.message().write(out, serializationCache, data.version());
    }

    @Override
    public ApiMessageAndVersion read(Readable input,
                                     int size) {
        short frameVersion = unsignedIntToShort(input, "frame version");

        if (frameVersion == 0) {
            throw new MetadataParseException("Could not deserialize metadata record with frame version 0. " +
                "Note that upgrades from the preview release of KRaft in 2.8 to newer versions are not supported.");
        } else if (frameVersion != DEFAULT_FRAME_VERSION) {
            throw new MetadataParseException("Could not deserialize metadata record due to unknown frame version "
                    + frameVersion + "(only frame version " + DEFAULT_FRAME_VERSION + " is supported)");
        }
        short apiKey = unsignedIntToShort(input, "type");
        short version = unsignedIntToShort(input, "version");

        ApiMessage record;
        try {
            record = apiMessageFor(apiKey);
        } catch (Exception e) {
            throw new MetadataParseException(e);
        }
        try {
            record.read(input, version);
        } catch (Exception e) {
            throw new MetadataParseException("Failed to deserialize record with type " + apiKey, e);
        }
        if (input.remaining() > 0) {
            throw new MetadataParseException("Found " + input.remaining() +
                    " byte(s) of garbage after " + apiKey);
        }
        return new ApiMessageAndVersion(record, version);
    }

    /**
     * Return {@code ApiMessage} instance for the given {@code apiKey}. This is used while deserializing the bytes
     * payload into the respective {@code ApiMessage} in {@link #read(Readable, int)} method.
     *
     * @param apiKey apiKey for which a {@code ApiMessage} to be created.
     */
    public abstract ApiMessage apiMessageFor(short apiKey);
}
