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

package org.apache.kafka.metadata;

import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public class MetadataParser {
    public static final int MAX_SERIALIZED_EVENT_SIZE = 32 * 1024 * 1024;

    private static short unsignedIntToShort(int val, String entity) {
        if (val > Short.MAX_VALUE) {
            throw new MetadataParseException("Value for " + entity + " was too large.");
        }
        return (short) val;
    }

    /**
     * Parse the given buffer.
     *
     * @param buffer    The buffer.  Its offsets will be modified.
     *
     * @return          The metadata message.
     */
    public static ApiMessage read(ByteBuffer buffer) {
        short type;
        try {
            type = unsignedIntToShort(ByteUtils.readUnsignedVarint(buffer), "type");
        } catch (Exception e) {
            throw new MetadataParseException("Failed to read variable-length type " +
                "number: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        short version;
        try {
            version = unsignedIntToShort(ByteUtils.readUnsignedVarint(buffer), "version");
        } catch (Exception e) {
            throw new MetadataParseException("Failed to read variable-length " +
                "version number: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        MetadataRecordType recordType = MetadataRecordType.fromId(type);
        ApiMessage message = recordType.newMetadataRecord();
        try {
            message.read(new ByteBufferAccessor(buffer), version);
        } catch (Exception e) {
            throw new MetadataParseException(recordType + "#parse failed: " +
                e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        if (buffer.hasRemaining()) {
            throw new MetadataParseException("Found " + buffer.remaining() +
                " byte(s) of garbage after " + recordType);
        }
        return message;
    }

    /**
     * Find the size of an API message and set up its ObjectSerializationCache.
     *
     * @param message   The metadata message.
     * @param version   The metadata message version.
     * @param cache     The object serialization cache to use.
     *
     * @return          The size
     */
    public static int size(ApiMessage message,
                           short version,
                           ObjectSerializationCache cache) {
        long messageSize = message.size(cache, version);
        long totalSize = messageSize +
            ByteUtils.sizeOfUnsignedVarint(message.apiKey()) +
            ByteUtils.sizeOfUnsignedVarint(version);
        if (totalSize > MAX_SERIALIZED_EVENT_SIZE) {
            throw new RuntimeException("Event size would be " + totalSize + ", but the " +
                "maximum serialized event size is " + MAX_SERIALIZED_EVENT_SIZE);
        }
        return (int) totalSize;
    }

    /**
     * Convert the given metadata message into a ByteBuffer.
     *
     * @param message   The metadata message.
     * @param version   The metadata message version.
     * @param cache     The object serialization cache to use.  This must have been
     *                  initialized by calling size() previously.
     * @param buf       The buffer to write to.
     */
    public static void write(ApiMessage message,
                             short version,
                             ObjectSerializationCache cache,
                             ByteBuffer buf) {
        ByteUtils.writeUnsignedVarint(message.apiKey(), buf);
        ByteUtils.writeUnsignedVarint(version, buf);
        message.write(new ByteBufferAccessor(buf), cache, version);
    }
}
