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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupCurrentMemberAssignmentValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupPartitionMetadataValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMemberValue;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataKey;
import org.apache.kafka.coordinator.group.generated.ConsumerGroupTargetAssignmentMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.group.runtime.PartitionWriter;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * Serializer/Deserializer for {@link Record}. The format is defined below:
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
public class RecordSerde implements PartitionWriter.Serializer<Record>, CoordinatorLoader.Deserializer<Record> {
    @Override
    public byte[] serializeKey(Record record) {
        // Record does not accept a null key.
        return MessageUtil.toVersionPrefixedBytes(
            record.key().version(),
            record.key().message()
        );
    }

    @Override
    public byte[] serializeValue(Record record) {
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
    public Record deserialize(
        ByteBuffer keyBuffer,
        ByteBuffer valueBuffer
    ) throws RuntimeException {
        final short recordType = readVersion(keyBuffer, "key");
        final ApiMessage keyMessage = apiMessageKeyFor(recordType);
        readMessage(keyMessage, keyBuffer, recordType, "key");

        if (valueBuffer == null) {
            return new Record(new ApiMessageAndVersion(keyMessage, recordType), null);
        }

        final ApiMessage valueMessage = apiMessageValueFor(recordType);
        final short valueVersion = readVersion(valueBuffer, "value");
        readMessage(valueMessage, valueBuffer, valueVersion, "value");

        return new Record(
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

    private ApiMessage apiMessageKeyFor(short recordType) {
        switch (recordType) {
            case 0:
            case 1:
                return new OffsetCommitKey();
            case 2:
                return new GroupMetadataKey();
            case 3:
                return new ConsumerGroupMetadataKey();
            case 4:
                return new ConsumerGroupPartitionMetadataKey();
            case 5:
                return new ConsumerGroupMemberMetadataKey();
            case 6:
                return new ConsumerGroupTargetAssignmentMetadataKey();
            case 7:
                return new ConsumerGroupTargetAssignmentMemberKey();
            case 8:
                return new ConsumerGroupCurrentMemberAssignmentKey();
            default:
                throw new CoordinatorLoader.UnknownRecordTypeException(recordType);
        }
    }

    private ApiMessage apiMessageValueFor(short recordType) {
        switch (recordType) {
            case 0:
            case 1:
                return new OffsetCommitValue();
            case 2:
                return new GroupMetadataValue();
            case 3:
                return new ConsumerGroupMetadataValue();
            case 4:
                return new ConsumerGroupPartitionMetadataValue();
            case 5:
                return new ConsumerGroupMemberMetadataValue();
            case 6:
                return new ConsumerGroupTargetAssignmentMetadataValue();
            case 7:
                return new ConsumerGroupTargetAssignmentMemberValue();
            case 8:
                return new ConsumerGroupCurrentMemberAssignmentValue();
            default:
                throw new CoordinatorLoader.UnknownRecordTypeException(recordType);
        }
    }
}
