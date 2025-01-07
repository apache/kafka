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

package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Serializer/Deserializer for {@link CoordinatorRecord}. The format is defined below:
 * <pre>
 *     record_key   = [record_type key_message]
 *     record_value = [value_version value_message]
 *
 *     record_type     : The record type is currently define as the version of the key
 *                       {@link ApiMessageAndVersion} object.
 *     key_message     : The serialized message of the key {@link ApiMessageAndVersion} object.
 *     value_version   : The value version is currently define as the version of the value
 *                       {@link ApiMessageAndVersion} object.
 *     value_message   : The serialized message of the value {@link ApiMessageAndVersion} object.
 * </pre>
 */
public abstract class CoordinatorRecordSerde implements Serializer<CoordinatorRecord>, Deserializer<CoordinatorRecord> {
    @Override
    public byte[] serializeKey(CoordinatorRecord record) {
        // Record does not accept a null key.
        return MessageUtil.toVersionPrefixedBytes(
            record.key().version(),
            record.key().message()
        );
    }

    @Override
    public byte[] serializeValue(CoordinatorRecord record) {
        // Tombstone is represented with a null value.
        if (record.value() == null) {
            return null;
        } else {
            return MessageUtil.toVersionPrefixedBytes(
                record.value().version(),
                record.value().message()
            );
        }
    }

    @Override
    public CoordinatorRecord deserialize(
        ByteBuffer keyBuffer,
        ByteBuffer valueBuffer
    ) throws RuntimeException {
        final short recordType = readVersion(keyBuffer, "key");
        final ApiMessage keyMessage = apiMessageKeyFor(recordType);
        readMessage(keyMessage, keyBuffer, recordType, "key");

        if (valueBuffer == null) {
            return new CoordinatorRecord(new ApiMessageAndVersion(keyMessage, recordType), null);
        }

        final ApiMessage valueMessage = apiMessageValueFor(recordType);
        final short valueVersion = readVersion(valueBuffer, "value");
        readMessage(valueMessage, valueBuffer, valueVersion, "value");

        return new CoordinatorRecord(
            new ApiMessageAndVersion(keyMessage, recordType),
            new ApiMessageAndVersion(valueMessage, valueVersion)
        );
    }

    private short readVersion(ByteBuffer buffer, String name) throws RuntimeException {
        try {
            return buffer.getShort();
        } catch (BufferUnderflowException ex) {
            throw new RuntimeException(String.format("Could not read version from %s's buffer.", name));
        }
    }

    private void readMessage(ApiMessage message, ByteBuffer buffer, short version, String name) throws RuntimeException {
        try {
            message.read(new ByteBufferAccessor(buffer), version);
        } catch (RuntimeException ex) {
            throw new RuntimeException(String.format("Could not read record with version %d from %s's buffer due to: %s.",
                version, name, ex.getMessage()), ex);
        }
    }

    /**
     * Concrete child class must provide implementation which returns appropriate
     * type of {@link ApiMessage} objects representing the key.
     *
     * @param recordVersion - short representing version
     * @return ApiMessage object
     */
    protected abstract ApiMessage apiMessageKeyFor(short recordVersion);

    /**
     * Concrete child class must provide implementation which returns appropriate
     * type of {@link ApiMessage} objects representing the value.
     *
     * @param recordVersion - short representing version
     * @return ApiMessage object
     */
    protected abstract ApiMessage apiMessageValueFor(short recordVersion);
}
